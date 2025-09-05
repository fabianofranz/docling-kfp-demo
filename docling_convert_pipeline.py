from kfp import dsl, compiler

from docling_convert_components import (
    import_pdfs,
    create_pdf_splits,
    download_docling_models,
    docling_convert,
)

@dsl.pipeline(
    name= "data-processing-docling-pipeline",
    description= "Docling convert pipeline by the Data Processing Team",
)
def convert_pipeline(
    num_splits: int = 3,
    pdf_from_s3: bool = False,
    pdf_filenames: str = "2203.01017v2.pdf,2206.01062.pdf,2305.03393v1-pg9.pdf,2305.03393v1.pdf,amt_handbook_sample.pdf,code_and_formula.pdf,multi_page.pdf,redp5110_sampled.pdf",
    # URL source params
    pdf_base_url: str = "https://github.com/docling-project/docling/raw/v2.43.0/tests/data/pdf",
    # S3 source params
    pdf_s3_endpoint: str = "https://s3.us-east-2.amazonaws.com",
    pdf_s3_access_key: str = "REDACTED",
    pdf_s3_secret_key: str = "REDACTED",
    pdf_s3_bucket: str = "my-bucket",
    pdf_s3_prefix: str = "my-pdfs",
    # Docling params
    docling_pdf_backend: str = "dlparse_v4",
    docling_image_export_mode: str = "embedded",
    docling_table_mode: str = "accurate",
    docling_num_threads: int = 4,
    docling_timeout_per_document: int = 300,
    docling_remote_model_enabled: bool = False,
    docling_ocr: bool = True,
    docling_force_ocr: bool = False,
    docling_ocr_engine: str = "easyocr",
    docling_allow_external_plugins: bool = False,
):
    from kfp import kubernetes

    importer = import_pdfs(
        filenames=pdf_filenames,
        base_url=pdf_base_url,
        from_s3=pdf_from_s3,
        s3_endpoint=pdf_s3_endpoint,
        s3_access_key=pdf_s3_access_key,
        s3_secret_key=pdf_s3_secret_key,
        s3_bucket=pdf_s3_bucket,
        s3_prefix=pdf_s3_prefix,
    )
    importer.set_caching_options(False)

    pdf_splits = create_pdf_splits(
        input_path=importer.outputs["output_path"],
        num_splits=num_splits,
    )

    artifacts = download_docling_models()
    artifacts.set_caching_options(False)

    with dsl.ParallelFor(pdf_splits.output) as pdf_split:
        remote_model_secret_mount_path = "/mnt/secrets"
        converter = docling_convert(
            input_path=importer.outputs["output_path"],
            artifacts_path=artifacts.outputs["output_path"],
            pdf_filenames=pdf_split,
            pdf_backend=docling_pdf_backend,
            image_export_mode=docling_image_export_mode,
            table_mode=docling_table_mode,
            num_threads=docling_num_threads,
            timeout_per_document=docling_timeout_per_document,
            remote_model_enabled=docling_remote_model_enabled,
            remote_model_secret_mount_path=remote_model_secret_mount_path,
            ocr=docling_ocr,
            force_ocr=docling_force_ocr,
            ocr_engine=docling_ocr_engine,
            allow_external_plugins=docling_allow_external_plugins,
        )
        converter.set_caching_options(False)
        converter.set_memory_request("1G")
        converter.set_memory_limit("6G")
        converter.set_cpu_request("500m")
        converter.set_cpu_limit("4")
        kubernetes.use_secret_as_volume(converter,
            secret_name="data-processing-docling-pipeline",
            mount_path=remote_model_secret_mount_path,
            optional=True)


if __name__ == "__main__":
    output_yaml = "docling_convert_pipeline_compiled.yaml"
    compiler.Compiler().compile(convert_pipeline, output_yaml)
    print(f"Docling pipeline compiled to {output_yaml}")
