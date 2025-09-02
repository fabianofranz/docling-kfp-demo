from pathlib import Path

from kfp import compiler

from docling_convert_pipeline import convert_pipeline


def test_pipeline_compiles(tmp_path: Path) -> None:
    out = tmp_path / "pipeline.yaml"
    compiler.Compiler().compile(convert_pipeline, str(out))
    assert out.exists() and out.stat().st_size > 0
