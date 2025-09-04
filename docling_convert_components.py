from typing import List

from kfp import dsl

PYTHON_BASE_IMAGE = "registry.access.redhat.com/ubi9/python-311:9.6-1755074620"
DOCLING_BASE_IMAGE = "quay.io/fabianofranz/docling-ubi9:2.45.0"


@dsl.component(
    base_image=PYTHON_BASE_IMAGE,
    packages_to_install=["boto3", "requests"],
)
def import_pdfs(
    output_path: dsl.Output[dsl.Artifact],
    filenames: str,
    base_url: str,
    from_s3: bool = False,
):
    """
    Import PDF filenames (comma-separated) from specified URL or S3 bucket.

    Args:
        filenames: List of PDF filenames to import.
        base_url: Base URL of the PDF files.
        output_path: Path to the output directory for the PDF files.
        from_s3: Whether or not to import from S3. Gets S3 credentials from environment variables.
    """
    import os # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    import boto3 # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from pathlib import Path # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    import requests # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402

    filenames_list = [name.strip() for name in filenames.split(",") if name.strip()]
    if not filenames_list:
        raise ValueError("filenames must contain at least one filename (comma-separated)")

    output_path_p = Path(output_path.path)
    output_path_p.mkdir(parents=True, exist_ok=True)

    if from_s3:
        s3_bucket = os.environ.get("BUCKET_NAME")
        s3_endpoint = os.environ.get("ENDPOINT_URL")
        s3_access_key = os.environ.get("ACCESS_KEY_ID")
        s3_secret_key = os.environ.get("SECRET_ACCESS_KEY")
        s3_prefix = os.environ.get("PREFIX")

        if not s3_endpoint:
            raise ValueError("s3_endpoint must be provided")

        if not s3_bucket:
            raise ValueError("s3_bucket must be provided")

        s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )
        
        for filename in filenames_list:
            orig = f"{s3_prefix.rstrip('/') if s3_prefix else ''}/{filename.lstrip('/')}"
            dest = output_path_p / filename
            print(f"import-test-pdfs: downloading {orig} -> {dest} from s3", flush=True)
            s3_client.download_file(s3_bucket, orig, dest)
    else:
        if not base_url:
            raise ValueError("base_url must be provided")

        for filename in filenames_list:
            url = f"{base_url.rstrip('/')}/{filename.lstrip('/')}"
            dest = output_path_p / filename
            print(f"import-test-pdfs: downloading {url} -> {dest}", flush=True)
            with requests.get(url, stream=True, timeout=30) as resp:
                resp.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
    
    print("import-test-pdfs: done", flush=True)


@dsl.component(
    base_image=PYTHON_BASE_IMAGE,
)
def create_pdf_splits(
    input_path: dsl.Input[dsl.Artifact],
    num_splits: int,
) -> List[List[str]]:
    """
    Create a list of PDF splits.

    Args:
        input_path: Path to the input directory containing PDF files.
        num_splits: Number of splits to create.
    """
    from pathlib import Path # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402

    input_path_p = Path(input_path.path)

    all_pdfs = [path.name for path in input_path_p.glob("*.pdf")]
    all_splits = [all_pdfs[i::num_splits] for i in range(num_splits)]
    filled_splits = list(filter(None, all_splits))
    return filled_splits


@dsl.component(
    base_image=DOCLING_BASE_IMAGE,
)
def download_docling_models(
    output_path: dsl.Output[dsl.Artifact],
):
    """
    Download Docling models.

    Args:
        output_path: Path to the output directory for Docling models.
    """
    from pathlib import Path # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.utils.model_downloader import download_models # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402

    output_path_p = Path(output_path.path)

    output_path_p.mkdir(parents=True, exist_ok=True)

    download_models(
        output_dir=output_path_p,
        progress=True,
        with_layout=True,
        with_tableformer=True,
        with_easyocr=True,
    )


@dsl.component(
    base_image=DOCLING_BASE_IMAGE,
)
def docling_convert(
    input_path: dsl.Input[dsl.Artifact],
    artifacts_path: dsl.Input[dsl.Artifact],
    output_path: dsl.Output[dsl.Artifact],
    pdf_filenames: List[str],
    pdf_backend: str = "dlparse_v4",
    image_export_mode: str = "embedded",
    table_mode: str = "accurate",
    num_threads: int = 4,
    timeout_per_document: int = 300,
    remote_model_enabled: bool = False,
    remote_model_endpoint_url: str = "",
    remote_model_api_key: str = "",
    remote_model_name: str = "",
):
    """
    Convert a list of PDF files to JSON and Markdown using Docling.

    Args:
        input_path: Path to the input directory containing PDF files.
        artifacts_path: Path to the directory containing Docling models.
        output_path: Path to the output directory for converted JSON and Markdown files.
        pdf_filenames: List of PDF file names to process.
        pdf_backend: Backend to use for PDF processing.
        image_export_mode: Mode to export images.
        table_mode: Mode to detect tables.
        num_threads: Number of threads to use per document processing.
        timeout_per_document: Timeout per document processing.
        remote_model_enabled: Whether or not to use a remote model.
        remote_model_endpoint_url: URL of the remote model.
        remote_model_api_key: API key or token for the remote model.
        remote_model_name: Name of the remote model.
    """
    import os
    from importlib import import_module
    from pathlib import Path

    from docling_core.types.doc.base import ImageRefMode  # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.datamodel.base_models import InputFormat  # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.datamodel.pipeline_options import (  # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
        PdfPipelineOptions,
        PdfBackend,
        TableFormerMode,
        VlmPipelineOptions,
    )
    from docling.document_converter import DocumentConverter, PdfFormatOption  # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions  # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.datamodel.pipeline_options_vlm_model import ApiVlmOptions, ResponseFormat # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.pipeline.vlm_pipeline import VlmPipeline # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402
    from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline # pylint: disable=import-outside-toplevel  # noqa: PLC0415, E402

    if not pdf_filenames:
        raise ValueError("pdf_filenames must be provided with the list of file names to process")

    allowed_pdf_backends = {e.value for e in PdfBackend}
    if pdf_backend not in allowed_pdf_backends:
        raise ValueError(
            f"Invalid pdf_backend: {pdf_backend}. Must be one of {sorted(allowed_pdf_backends)}"
        )

    allowed_table_modes = {e.value for e in TableFormerMode}
    if table_mode not in allowed_table_modes:
        raise ValueError(
            f"Invalid table_mode: {table_mode}. Must be one of {sorted(allowed_table_modes)}"
        )

    allowed_image_export_modes = {e.value for e in ImageRefMode}
    if image_export_mode not in allowed_image_export_modes:
        raise ValueError(
            f"Invalid image_export_mode: {image_export_mode}. Must be one of {sorted(allowed_image_export_modes)}"
        )

    input_path_p = Path(input_path.path)
    artifacts_path_p = Path(artifacts_path.path)
    output_path_p = Path(output_path.path)
    output_path_p.mkdir(parents=True, exist_ok=True)

    input_pdfs = [input_path_p / name for name in pdf_filenames]
    print(f"docling-convert: starting with backend='{pdf_backend}', files={len(input_pdfs)}", flush=True)

    if remote_model_enabled:
        if not remote_model_endpoint_url:
            raise ValueError("remote_model_endpoint_url must be provided when remote_model_enabled is True")

        pipeline_options = VlmPipelineOptions(
            enable_remote_services=True,
        )
        pipeline_options.vlm_options = ApiVlmOptions(
            url=remote_model_endpoint_url, # type: ignore[arg-type]
            params=dict(
                model_id=remote_model_name,
                parameters=dict(
                    max_new_tokens=400,
                ),
            ),
            prompt="OCR the full page to markdown.",
            timeout=360,
            response_format=ResponseFormat.MARKDOWN,
            # TODO: remote_model_api_key should be something other than a KFP param (maybe a secret?), so it's not exposed in the UI
            headers={
                "Authorization": f"Bearer {remote_model_api_key}",
            },
        )

    else:
        pipeline_options = PdfPipelineOptions()
        pipeline_options.artifacts_path = artifacts_path_p
        pipeline_options.do_ocr = True
        pipeline_options.do_table_structure = True
        pipeline_options.table_structure_options.do_cell_matching = True
        pipeline_options.generate_page_images = True
        pipeline_options.table_structure_options.mode = TableFormerMode(table_mode)

    pipeline_options.document_timeout = float(timeout_per_document)
    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=num_threads, device=AcceleratorDevice.AUTO
    )

    backend_to_impl = {
        PdfBackend.PYPDFIUM2.value: (
            "docling.backend.pypdfium2_backend",
            "PyPdfiumDocumentBackend",
        ),
        PdfBackend.DLPARSE_V1.value: (
            "docling.backend.docling_parse_backend",
            "DoclingParseDocumentBackend",
        ),
        PdfBackend.DLPARSE_V2.value: (
            "docling.backend.docling_parse_v2_backend",
            "DoclingParseV2DocumentBackend",
        ),
        PdfBackend.DLPARSE_V4.value: (
            "docling.backend.docling_parse_v4_backend",
            "DoclingParseV4DocumentBackend",
        ),
    }

    module_name, class_name = backend_to_impl[pdf_backend]
    backend_class = getattr(import_module(module_name), class_name)

    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
                backend=backend_class,
                pipeline_cls=VlmPipeline if remote_model_enabled else StandardPdfPipeline,
            )
        }
    )

    easyocr_path_p = artifacts_path_p / "EasyOcr"
    os.environ["MODULE_PATH"] = str(easyocr_path_p)
    os.environ["EASYOCR_MODULE_PATH"] = str(easyocr_path_p)

    results = doc_converter.convert_all(input_pdfs, raises_on_error=True)

    for result in results:
        doc_filename = result.input.file.stem

        output_json_path = output_path_p / f"{doc_filename}.json"
        print(f"docling-convert: saving {output_json_path}", flush=True)
        result.document.save_as_json(output_json_path, image_mode=ImageRefMode(image_export_mode))

        output_md_path = output_path_p / f"{doc_filename}.md"
        print(f"docling-convert: saving {output_md_path}", flush=True)
        result.document.save_as_markdown(output_md_path, image_mode=ImageRefMode(image_export_mode))

    print("docling-convert: done", flush=True)
