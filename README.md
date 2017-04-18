# RedPipe
Make redis pipelines easier to use in python.

[![Build Status](https://travis-ci.org/72squared/redpipe.svg?branch=master)](https://travis-ci.org/72squared/redpipe) [![Coverage Status](https://coveralls.io/repos/github/72squared/redpipe/badge.svg?branch=master)](https://coveralls.io/github/72squared/redpipe?branch=master)

## Basics
```python
import redpipe
import redis

# open a connection to redis
client = redis.StrictRedis()

# configure redpipe by passing that connection in.
redpipe.connect(client)

# create a pipeline context.
with redpipe.PipelineContext() as pipe:
    # do a bunch of operations in a pipeline
    key1 = pipe.incrby('key1', '1')
    pipe.expire('key1', 60)
    key2 = pipe.incrby('key2', '3')
    pipe.expire('key2', 60)
    

# when we exit the pipeline context, the pipeline
# executes automatically, and the objects we collected
# get hydrated with the results.

# prints the response from redis: INCRBY key1 1
print(key1.result)

# prints the response from redis: INCRBY key2 3
print(key2.result)
```

## ORM
```python
import redpipe
import redis
from time import time

# configure redpipe. 
# only need to do this once in your application.
redpipe.connect(redis.StrictRedis())

# set up a model object.
class User(redpipe.Model):
    _namespace = 'U'
    _fields = {
        'name': redpipe.TextField,
        'last_name': redpipe.TextField,
        'last_seen': redpipe.IntegerField,
        'admin': redpipe.BooleanField,
    }

    @property
    def user_id(self):
        return self.key


# now let's use the model.
with redpipe.PipelineContext() as pipe:
    # create a few users
    u1 = User('1', name='Bob', last_seen=int(time()), pipe=pipe)
    u2 = User('2', name='Jill', last_seen=int(time()), pipe=pipe)

print("first batch: %s" % [dict(u1), dict(u2)])

# when we exit the context, all the models are saved to redis
# in one pipeline operation.
# now let's read those two users we created and modify them
with redpipe.PipelineContext() as pipe:
    users = [User('1', pipe=pipe), User('2', pipe=pipe)]
    users[0].save(name='Bobby', last_seen=int(time()), pipe=pipe)

print("second batch: %s" % [dict(u1), dict(u2)])

```