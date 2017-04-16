
class Connector(object):

    __slots__ = ['get']

    def connect_pipeline(self, pipeline_method):
        self.get = pipeline_method

    def connect(self, redis_client):
        self.connect_pipeline(redis_client.pipeline)

connector = Connector()

connect = connector.connect
connect_pipeline = connector.connect_pipeline
