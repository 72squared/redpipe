import redislite
import json
import time
from rediswrap import Pipeline, NestedPipeline

# the redis client connection for our app
CLIENT = redislite.StrictRedis()


def pipeline(p=None):
    """
    utility function to allow us to easily nest pipelines
    :param p:
    :return:
    """
    return Pipeline(CLIENT.pipeline()) if p is None else NestedPipeline(p)


class Pipe(object):
    """
    Allow us to declare a with block and automatically execute on exiting
    the block statement.

    example:
        with Pipe() as pipe:
            pipe.zadd(key, score, element)

    this is equivalent to writing:
        pipe = pipeline()
        pipe.zadd(key, score, element)
        pipe.execute()

    """
    __slots__ = ['_pipe']

    def __init__(self, pipe=None):
        self._pipe = pipeline(pipe)

    def __enter__(self, pipe=None):
        return self._pipe

    def __exit__(self, type, value, traceback):
        if type is None:
            self._pipe.execute()
        self._pipe.reset()


class Followers(object):
    """
    A class for storing followers as a sorted set.
    """
    __slots__ = ['followed_id']

    def __init__(self, followed_id):
        """
        keep track of the primary key
        :param followed_id:
        """
        self.followed_id = followed_id

    @property
    def key(self):
        """
        format for the database
        :return:
        """
        return "F{%s}" % self.followed_id

    def add(self, follower_id, pipe=None):
        """
        add a follower
        :param follower_id: str
        :param pipe: pipeline()
        :return:
        """
        with Pipe(pipe) as pipe:
            return pipe.zadd(self.key, time.time(), follower_id)

    def remove(self, follower_id, pipe):
        """
        remove a follower
        :param follower_id:
        :param pipe:
        :return:
        """
        with Pipe(pipe) as pipe:
            return pipe.zrem(self.key, follower_id)

    def range(self, offset=0, limit=-1, pipe=None):
        """
        get a subset of the followers.
        :param offset:
        :param limit:
        :param pipe:
        :return:
        """
        result = []
        with Pipe(pipe) as pipe:
            ref = pipe.zrange(self.key, offset, limit)

            def cb():
                for v in ref.result:
                    result.append(v)

            pipe.on_execute(cb)
        return result

    def all(self, pipe=None):
        """
        get all of the followers
        :param pipe:
        :return:
        """
        return self.range(pipe=pipe)


class User(object):
    __slots__ = ['user_id', 'first_name', 'last_name', 'email',
                 '_persisted']

    def __init__(self, user_id, pipe=None, **kwargs):
        self.user_id = user_id
        self._persisted = False
        if kwargs:
            self.first_name = kwargs.get('first_name')
            self.last_name = kwargs.get('last_name')
            self.email = kwargs.get('email')
        else:
            with Pipe(pipe) as pipe:
                ref = pipe.hmget(
                    self.key,
                    ['first_name', 'last_name', 'email'])

                def cb():
                    if any(v is not None for v in ref.result):
                        self.first_name = ref.result[0]
                        self.last_name = ref.result[1]
                        self.email = ref.result[2]
                        self._persisted = True

                pipe.on_execute(cb)

    @property
    def key(self):
        return "U{%s}" % self.user_id

    @property
    def persisted(self):
        return self._persisted

    def _validate(self):
        """
        make sure the data is valid
        :return:
        """
        pass

    def save(self, pipe=None):
        self._validate()
        if not all([self.first_name, self.last_name, self.email]):
            raise RuntimeError('invalid user data')
        with Pipe(pipe) as pipe:
            pipe.hmset(
                self.key,
                {
                    'first_name': self.first_name,
                    'last_name': self.last_name,
                    'email': self.email,
                }
            )

            def cb():
                self._persisted = True

            pipe.on_execute(cb)

    def delete(self, pipe=None):
        with Pipe(pipe) as pipe:
            pipe.hdel(self.key, *['first_name', 'last_name', 'email'])

            def cb():
                self._persisted = False
                del self.first_name
                del self.last_name
                del self.email

            pipe.on_execute(cb)

    def __repr__(self):
        if self.persisted:
            return json.dumps({
                'user_id': self.user_id,
                'first_name': self.first_name,
                'last_name': self.last_name,
                'email': self.email,
            })
        else:
            return ''


def test_user(k, pipe=None):
    """
    a utility function to save a test user
    :param k:
    :param pipe:
    :return:
    """
    u = User(
        user_id="%s" % k,
        first_name='first%s' % k,
        last_name='last%s' % k,
        email='user%s@test.com' % k
    )
    u.save(pipe=pipe)
    return u


if __name__ == '__main__':

    with Pipe() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]

        print("list of users before execute: %s" % users)

    print("list of users after execute: %s" % users)

    user = User('1')
    print("user 1: %s" % user)

    user.delete()
    print("user 1 after delete: %s" % user)

    with Pipe() as pipe:

        # create a bunch of test users
        users = [test_user(k, pipe=pipe) for k in range(1, 3)]

        # get them all from the database
        users = [User(u.user_id, pipe=pipe) for u in users]

        # create a list of followers for user 1
        f = Followers('1')

        # add some followers by id
        for n in range(2, 5):
            f.add("%s" % n, pipe=pipe)

        # get all of the followers we added
        result = f.all(pipe=pipe)

        # now we run all of this in one big pipeline statement.
        # so far, nothing has hit the database.
        # Amazing, right?
        print("list of users before execute: %s" % result)

    # print out the list of the generated users
    print("test users after execute:")

    for u in users:
        print("    %s" % u)

    print("followers: %s" % result)
