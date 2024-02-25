import os
import sys

sys.path.append("..")

from flask import Flask, request, make_response
from edfs.edfs_client import EDFSClient

UPLOADED_FILES_DIR = "./upload"

app = Flask(__name__)

@app.route('/')
def root():
    return app.send_static_file("index.html")

@app.route("/files",  methods=["GET"])
async def get_all_files():
    edfs_client = await EDFSClient.create()
    files = await edfs_client.get_all_files()
    return files

@app.route("/file/<path:filepath>",  methods=["GET"])
async def get_file(filepath):
    print(filepath)
    edfs_client = await EDFSClient.create()
    data = await edfs_client.get_file(filepath)
    if not data.get("success"):
         response = make_response("", 404)
    else:
        content = data.get("file")
        response = make_response(content, 200)
        response.mimetype = "text/plain"
    return response

@app.route('/upload/', methods=["POST"])
@app.route("/upload/<path:des>",  methods=["POST"])
async def upload_file(des=""):
    file = request.files['file']

    if not os.path.exists(UPLOADED_FILES_DIR):
        os.makedirs(UPLOADED_FILES_DIR)

    src = f'{UPLOADED_FILES_DIR}/{file.filename}'
    file.save(src)

    edfs_client = await EDFSClient.create()
    if await edfs_client.put(src, des):
        response = make_response({"success": True}, 200)
    else:
        response = make_response({"success": False}, 200)

    os.remove(src)

    return response


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)