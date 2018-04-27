from app import app
from werkzeug.serving import WSGIRequestHandler
import socket

run_mode = "local"
# run_mode = "server"

if __name__ == '__main__':
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    if run_mode == "local":
        app.run(debug=True)
    else:
        app.run(host= '0.0.0.0', port=80, debug=True)
