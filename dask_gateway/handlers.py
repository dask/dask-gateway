from tornado import web


class RootHandler(web.RequestHandler):
    def get(self):
        self.write("Hello world!")


default_handlers = [
    ("/", RootHandler)
]
