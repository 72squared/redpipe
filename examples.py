import redislite
import time
from rediswrap import Model, PipelineContext, \
    StringField, TextField, BooleanField, IntegerField, \
    connect

# the redis client connection for our app
CLIENT = redislite.StrictRedis()
connect(CLIENT)


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
        with PipelineContext(pipe) as pipe:
            return pipe.zadd(self.key, time.time(), follower_id)

    def remove(self, follower_id, pipe):
        """
        remove a follower
        :param follower_id:
        :param pipe:
        :return:
        """
        with PipelineContext(pipe) as pipe:
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
        with PipelineContext(pipe) as pipe:
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


class User(Model):
    _fields = {
        'first_name': StringField(),
        'last_name': StringField(),
        'email': TextField(),
        'beta_user': BooleanField(),
        'admin': BooleanField(),
        'last_seen': IntegerField,
    }

    @property
    def user_id(self):
        return self.key

    @property
    def first_name(self):
        return self._data['first_name']

    @property
    def last_name(self):
        return self._data['last_name']


def test_user(k, pipe=None):
    """
    a utility function to save a test user
    :param k:
    :param pipe:
    :return:
    """
    u = User(
        "%s" % k,
        first_name='first%s' % k,
        last_name='last%s' % k,
        email='user%s@test.com' % k
    )
    u.change(pipe=pipe)
    return u


if __name__ == '__main__':
    user = User(
        '1',
        first_name='John',
        last_name='Loehrer',
        email='72squared@gmail.com',
        beta_user=True)

    user.change(admin=True)

    with PipelineContext() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]

        print("list of users before execute: %s" % users)

    print("list of users after execute: %s" % users)

    user = User('1')
    print("user 1: %s" % user)

    user.delete()
    print("user 1 after delete: %s" % user)

    with PipelineContext() as pipe:

        # create a bunch of test users
        users = [test_user(k, pipe=pipe) for k in range(1, 3)]

        # get them all from the database
        users = [User(u.key, pipe=pipe) for u in users]

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
        print("    %s" % repr(u))

    print("followers: %s" % result)
