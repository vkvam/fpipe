import mimetypes


def guess_mime(path: str):
    mime, encoding = mimetypes.guess_type(path, False)
    if not mime:
        mime = "application/octet-stream"
    return mime, encoding
