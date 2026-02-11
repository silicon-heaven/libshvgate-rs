use std::path::Path;

use async_compression::tokio::write::GzipEncoder;
use futures::TryStreamExt as _;
use sha1::Digest as _;
use shvclient::clientnode::{LsHandlerResult, Method, RequestHandlerResult, UnresolvedRequest, err_unresolved_request};
use shvclient::shvproto::{MetaMap, RpcValue, make_list, rpcvalue};
use shvrpc::metamethod::{AccessLevel, MetaMethod};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;

const METH_HASH: &str = "hash";
const METH_SIZE: &str = "size";
const METH_READ: &str = "read";
const METH_READ_COMPRESSED: &str = "readCompressed";
const METH_LS_FILES: &str = "lsfiles";

const META_METHOD_LS_FILES: MetaMethod = MetaMethod::new_static(METH_LS_FILES, shvrpc::metamethod::Flags::None, AccessLevel::Read, "Map|Null", "List", &[], "");

fn rpc_error_filesystem(err: std::io::Error) -> RpcError {
    RpcError::new(
        RpcErrorCode::MethodCallException,
        format!("Filesystem error: {err}")
    )
}

fn is_path_safe(path: impl AsRef<Path>) -> bool {
    !path.as_ref()
        .components()
        .any(|component| matches!(
                component,
                std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        ))
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FileType {
    File,
    Directory,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            FileType::File => "f",
            FileType::Directory => "d",
        })
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LsFilesEntry {
    name: String,
    ftype: FileType,
    size: i64,
}

impl std::fmt::Display for LsFilesEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}, {}, {}}}", self.name, self.ftype, self.size)
    }
}

impl From<LsFilesEntry> for RpcValue {
    fn from(value: LsFilesEntry) -> Self {
        make_list!(value.name, value.ftype.to_string(), value.size).into()
    }
}

#[derive(Default)]
struct ReadParams {
    offset: Option<u64>,
    size: Option<u64>,
}

impl TryFrom<RpcValue> for ReadParams {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&RpcValue> for ReadParams {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        if value.is_null() {
            return Ok(Self::default());
        }

        let map: rpcvalue::Map = value.try_into()?;

        let parse_param = |param_name: &str| -> Result<Option<u64>, String> {
            match map.get(param_name) {
                None => Ok(None),
                Some(val) => {
                    i64::try_from(val)
                        .map_err(|e| e.to_string())
                        .and_then(|v| u64::try_from(v)
                            .map_err(|e| e.to_string())
                        )
                        .map_err(|e| format!("Error parsing `{param_name}` parameter: {e}"))
                        .map(Some)
                }
            }
        };

        let offset = parse_param("offset")?;
        let size = parse_param("size")?;

        Ok(Self { offset, size })
    }
}

fn rpc_error_method_not_found() -> RpcError {
    RpcError::new(RpcErrorCode::MethodNotFound, "Method not found")
}

async fn file_reader(
    path: impl AsRef<Path>,
    offset: u64,
    size: Option<u64>,
) -> std::io::Result<tokio::io::Take<tokio::io::BufReader<tokio::fs::File>>>
{
    const MAX_READ_SIZE: u64 = 1 << 20;
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let file_size = file.metadata().await?.len();
    let size = size.unwrap_or(file_size).min(file_size).min(MAX_READ_SIZE);
    Ok(tokio::io::BufReader::new(file).take(size))
}

async fn read_file(path: impl AsRef<Path>, offset: u64, size: Option<u64>) -> tokio::io::Result<Vec<u8>> {
    let mut reader = file_reader(path, offset, size).await?;
    let mut res = Vec::new();
    reader.read_to_end(&mut res).await?;
    Ok(res)
}

async fn compress_file(path: impl AsRef<Path>, offset: u64, size: Option<u64>) -> tokio::io::Result<(Vec<u8>, u64)> {
    let mut reader = file_reader(path, offset, size).await?;
    let mut res = Vec::new();
    let mut encoder = GzipEncoder::new(&mut res);
    let bytes_read = tokio::io::copy_buf(&mut reader, &mut encoder).await?;
    encoder.shutdown().await?;
    Ok((res, bytes_read))
}

pub async fn fs_request_handler(
    base_path: impl AsRef<Path>,
    sub_path: impl AsRef<str>,
    method: Method,
    param: Option<RpcValue>,
) -> RequestHandlerResult
{
    async fn get_dir_entries(path: impl AsRef<Path>) -> Result<ReadDirStream, RpcError> {
        Ok(ReadDirStream::new(
                tokio::fs::read_dir(path)
                .await
                .map_err(rpc_error_filesystem)?)
        )
    }

    async fn ls_handler(entries: ReadDirStream) -> LsHandlerResult {
        entries
            .try_filter_map(async |entry| {
                    Ok(entry.file_name().to_str().map(String::from))
                }
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(rpc_error_filesystem)
            .map(|mut res| { res.sort(); res})
    }

    async fn lsfiles_handler(entries: ReadDirStream) -> Result<Vec<LsFilesEntry>, RpcError> {
        entries
            .try_filter_map(
                async |entry| {
                    let res = async {
                        let meta = entry
                            .metadata()
                            .await
                            .inspect_err(|e| log::error!("Cannot read metadata of file `{}`: {}", entry.path().to_string_lossy(), e))
                            .ok()?;
                        let name = entry.file_name().to_str().map(String::from)?;
                        let ftype = if meta.is_dir() { FileType::Directory } else if meta.is_file() { FileType::File } else { return None };
                        let size = meta.len() as i64;
                        Some(LsFilesEntry { name, ftype, size })
                    }.await;
                    Ok(res)
                }
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(rpc_error_filesystem)
            .map(|mut res| {
                res.sort_by(|a, b| a.name.cmp(&b.name));
                res
            })
    }

    async fn file_methods_handler(method: &str, path: &Path, file_size: u64, param: &RpcValue) -> Result<RpcValue, RpcError> {
        match method {
            METH_HASH => {
                let file = tokio::fs::File::open(path)
                    .await
                    .map_err(rpc_error_filesystem)?;

                // 64 KiB buffer performs better for large files than the 8 KiB default
                const READER_CAPACITY: usize = 1 << 16;
                let mut file_stream = ReaderStream::with_capacity(file, READER_CAPACITY);
                let mut hasher = sha1::Sha1::new();
                while let Some(res) = file_stream.next().await {
                    let bytes = res.map_err(rpc_error_filesystem)?;
                    hasher.update(&bytes);
                }
                Ok(hex::encode(hasher.finalize()).into())
            }
            METH_SIZE => Ok((file_size as i64).into()),
            METH_READ => {
                let read_params: ReadParams = param
                    .try_into()
                    .map_err(|msg| RpcError::new(RpcErrorCode::InvalidParam, msg))?;
                let offset = read_params.offset.unwrap_or(0);
                let res = read_file(path, offset, read_params.size)
                    .await
                    .map_err(rpc_error_filesystem)?;
                let mut result_meta = MetaMap::new();
                result_meta
                    .insert("offset", (offset as i64).into())
                    .insert("size", (res.len() as i64).into());
                Ok(RpcValue::new(res.into(), Some(result_meta)))
            }
            METH_READ_COMPRESSED => {
                let read_params: ReadParams = param
                    .try_into()
                    .map_err(|msg| RpcError::new(RpcErrorCode::InvalidParam, msg))?;
                let offset = read_params.offset.unwrap_or(0);
                let (res, bytes_read) = compress_file(path, offset, read_params.size)
                    .await
                    .map_err(rpc_error_filesystem)?;
                let mut result_meta = MetaMap::new();
                result_meta
                    .insert("offset", (offset as i64).into())
                    .insert("size", (bytes_read as i64).into());
                Ok(RpcValue::new(res.into(), Some(result_meta)))
            }
            _ => Err(rpc_error_method_not_found()),
        }
    }

    let (base_path, sub_path) = (base_path.as_ref(), sub_path.as_ref());

    // Validate sub_path format
    if !is_path_safe(sub_path) {
        return err_unresolved_request()
    }

    let base_path = base_path
        .canonicalize()
        .inspect_err(|err| log::error!("Cannot resolve base path `{base_path}`: {err}", base_path = base_path.display()))
        .map_err(|_| UnresolvedRequest)?;
    let path = base_path
        .join(sub_path)
        .canonicalize()
        .map_err(|_| UnresolvedRequest)?;

    // Detect symlink traversal
    if !path.starts_with(base_path) {
        return err_unresolved_request()
    }

    // Probe the path on the fs. Prevent leaking FS info to unauthorized
    // users, do not return `rpc_error_filesystem`.
    let path_meta = tokio::fs::metadata(&path)
        .await
        .inspect_err(|err| log::error!("Cannot read FS metadata, path: {path}, error: {err}", path = path.display()))
        .or(Err(UnresolvedRequest))?;

    if path_meta.is_dir() {
        const METHODS: &[MetaMethod] = &[META_METHOD_LS_FILES];
        match method {
            Method::Dir(dir) => dir.resolve(METHODS),
            Method::Ls(ls) => ls.resolve(METHODS, async || {
                ls_handler(get_dir_entries(path).await?).await
            }),
            Method::Other(m) if m.method() == METH_LS_FILES => m.resolve(METHODS, async || {
                lsfiles_handler(get_dir_entries(path).await?).await
            }),
            _ => err_unresolved_request(),
        }
    } else if path_meta.is_file() {
        const METHODS: &[MetaMethod] = &[
            MetaMethod::new_static(METH_HASH, shvrpc::metamethod::Flags::None, AccessLevel::Read, "Map|Null", "String", &[], ""),
            MetaMethod::new_static(METH_SIZE, shvrpc::metamethod::Flags::None, AccessLevel::Browse, "", "Int", &[], ""),
            MetaMethod::new_static(METH_READ, shvrpc::metamethod::Flags::None, AccessLevel::Read, "Map", "Blob", &[], "Parameters\n  offset: file offset to start read, default is 0\n  size: number of bytes to read starting on offset, default is till end of file\n"),
            MetaMethod::new_static(METH_READ_COMPRESSED, shvrpc::metamethod::Flags::None, AccessLevel::Read, "Map", "Blob", &[], "Parameters\n  read() parameters\n  compressionType: gzip (default)"),
        ];
        match method {
            Method::Dir(dir) => dir.resolve(METHODS),
            Method::Ls(ls) => ls.resolve(METHODS, async || Ok(vec![])),
            Method::Other(m) => {
                let method = m.method().to_owned();
                m.resolve(METHODS, async move || {
                    file_methods_handler(&method, &path, path_meta.len(), &param.unwrap_or_else(RpcValue::null)).await
                })
            },
        }
    } else {
        err_unresolved_request()
    }
}
