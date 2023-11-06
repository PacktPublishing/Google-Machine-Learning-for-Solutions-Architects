from google.cloud import documentai
from google.api_core.client_options import ClientOptions

# Replace the following values and uncomment those lines
# project_id = 'YOUR_PROJECT_ID'
# location = 'YOUR_PROCESSOR_LOCATION' # 'us' or 'eu'
# processor_id = 'YOUR_PROCESSOR_ID' 
# file_path = '/path/to/file' # Example: './image.png'
# mime_type = 'image/png' 

def process_ocr(
    project_id: str, location: str, processor_id: str, file_path: str, mime_type: str
):
    # Create client
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    name = client.processor_path(project_id, location, processor_id)

    # Read input file
    with open(file_path, "rb") as image:
        image_content = image.read()
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)

    # Create the request object
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)

    # Process the request and store the results
    result = client.process_document(request=request)
    document = result.document

    # Print the extracted text
    print(document.text)

process_ocr(project_id, location, processor_id, file_path, mime_type)
