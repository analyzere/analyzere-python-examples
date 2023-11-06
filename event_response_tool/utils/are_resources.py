def check_resource_upload_status(are_resource):
    while True:
        are_resource.reload()
        if are_resource.status in [
            "processing_failed",
            "processing_succeeded",
        ]:
            break
