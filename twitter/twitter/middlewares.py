# Importing base64 library because we'll need it ONLY in case if the proxy we are going to use requires authentication
import base64

# Start your middleware class
class ProxyMiddleware(object):
    # overwrite process request
    def process_request(self, request, spider):
        # Set the location of the proxy
        request.meta['proxy'] = "http://103.239.30.188:80"

        # Use the following lines if your proxy requires authentication
        proxy_user_pass = "vpnuser01:1qaz@WSX"
        # setup basic authentication for the proxy
        encoded_user_pass = base64.encodestring(proxy_user_pass)
        
        request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
