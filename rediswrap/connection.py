from .exceptions import AlreadyConnected


class Connector(object):

    __slots__ = ['get']

    def connect_pipeline(self, pipeline_method):
        try:
            if self.get != pipeline_method:
                raise AlreadyConnected("can't change connection.")
            return
        except AttributeError:
            pass

        self.get = pipeline_method  # noqa

    def connect(self, redis_client):
        self.connect_pipeline(redis_client.pipeline)

    def disconnect(self):
        try:
            del self.get
        except AttributeError:
            pass


connector = Connector()
connect = connector.connect
connect_pipeline = connector.connect_pipeline
disconnect = connector.disconnect
