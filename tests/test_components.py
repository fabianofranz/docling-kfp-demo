from typing import Any, Iterator, List

from pathlib import Path

import builtins
import types
import pytest

from docling_convert_components import (
    import_pdfs,
    create_pdf_splits,
)


class _FakeArtifact:
    def __init__(self, path: Path) -> None:
        self.path = str(path)


def _write_files(dir_path: Path, names: List[str], content: bytes = b"x") -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    for n in names:
        (dir_path / n).write_bytes(content)


def test_create_pdf_splits_basic(tmp_path: Path) -> None:
    pdf_dir = tmp_path / "pdfs"
    files = [
        "a.pdf",
        "b.pdf",
        "c.pdf",
        "d.pdf",
        "e.pdf",
    ]
    _write_files(pdf_dir, files)

    splits = create_pdf_splits.python_func(input_path=_FakeArtifact(pdf_dir), num_splits=3)

    # All names present across splits
    flattened = [name for batch in splits for name in batch]
    assert sorted(flattened) == sorted(files)
    # No empty batches returned
    assert all(len(batch) > 0 for batch in splits)


def test_import_pdfs_url_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        def raise_for_status(self) -> None: ...
        def iter_content(self, chunk_size: int) -> Iterator[bytes]:
            yield from (b"hi", b"", b"there")
        def __enter__(self) -> "_Resp": return self
        def __exit__(self, *exc: Any) -> None: return None

    def fake_get(url: str, stream: bool, timeout: int) -> _Resp:  # type: ignore[override]
        return _Resp()

    import requests  # type: ignore
    monkeypatch.setattr(requests, "get", fake_get)

    out_dir = tmp_path / "out"
    import_pdfs.python_func(
        output_path=_FakeArtifact(out_dir),
        filenames="a.pdf, b.pdf",
        base_url="https://example.test/base",
        from_s3=False,
    )
    assert (out_dir / "a.pdf").read_bytes() == b"hithere"
    assert (out_dir / "b.pdf").read_bytes() == b"hithere"


def test_import_pdfs_s3_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, Path]] = []

    class _FakeS3:
        def download_file(self, bucket: str, key: str, filename: str) -> None:
            calls.append((bucket, key, Path(filename)))
            Path(filename).write_bytes(b"ok")

    def fake_boto3_client(name: str, **kwargs: Any) -> _FakeS3:  # type: ignore[override]
        return _FakeS3()

    import boto3  # type: ignore
    monkeypatch.setattr(boto3, "client", fake_boto3_client)

    out_dir = tmp_path / "out"
    import_pdfs.python_func(
        output_path=_FakeArtifact(out_dir),
        filenames="x.pdf, y.pdf",
        base_url="",
        from_s3=True,
        s3_endpoint="https://s3.us-east-2.amazonaws.com",
        s3_access_key="k",
        s3_secret_key="s",
        s3_bucket="bucket",
        s3_prefix="prefix",
    )
    assert (out_dir / "x.pdf").read_bytes() == b"ok"
    assert (out_dir / "y.pdf").read_bytes() == b"ok"
    assert calls == [
        ("bucket", "prefix/x.pdf", out_dir / "x.pdf"),
        ("bucket", "prefix/y.pdf", out_dir / "y.pdf"),
    ]


def test_import_pdfs_url_mode_via_wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mock requests like above
    class _Resp:
        def raise_for_status(self) -> None: ...
        def iter_content(self, chunk_size: int) -> Iterator[bytes]:
            yield from (b"A", b"B")
        def __enter__(self) -> "_Resp": return self
        def __exit__(self, *exc: Any) -> None: return None

    def fake_get(url: str, stream: bool, timeout: int) -> _Resp:  # type: ignore[override]
        return _Resp()

    import requests  # type: ignore
    monkeypatch.setattr(requests, "get", fake_get)

    out_dir = tmp_path / "out"
    import_pdfs.python_func(
        output_path=_FakeArtifact(out_dir),
        filenames="doc1.pdf, doc2.pdf",
        base_url="https://example.test/base",
        from_s3=False,
    )
    assert (out_dir / "doc1.pdf").read_bytes() == b"AB"
    assert (out_dir / "doc2.pdf").read_bytes() == b"AB"


def test_import_pdfs_s3_mode_via_wrapper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mock boto3 like above
    class _FakeS3:
        def download_file(self, bucket: str, key: str, filename: str) -> None:
            Path(filename).write_bytes(b"S3")

    def fake_boto3_client(name: str, **kwargs: Any) -> _FakeS3:  # type: ignore[override]
        return _FakeS3()

    import boto3  # type: ignore
    monkeypatch.setattr(boto3, "client", fake_boto3_client)

    out_dir = tmp_path / "out"
    import_pdfs.python_func(
        output_path=_FakeArtifact(out_dir),
        filenames="d1.pdf, d2.pdf",
        base_url="",
        from_s3=True,
        s3_endpoint="https://s3.us-east-2.amazonaws.com",
        s3_access_key="k",
        s3_secret_key="s",
        s3_bucket="b",
        s3_prefix="p",
    )
    assert (out_dir / "d1.pdf").read_bytes() == b"S3"
    assert (out_dir / "d2.pdf").read_bytes() == b"S3"


