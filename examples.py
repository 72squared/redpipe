import redislite
import time
import redpipe
from redpipe import Model, PipelineContext, \
    TextField, BooleanField, IntegerField, \
    connect_redis

# set up the redis client connection for our app
connect_redis(redislite.StrictRedis())
connect_redis(redislite.StrictRedis(), name='alt')


class Followers(redpipe.SortedSet):
    """
    A class for storing followers as a sorted set.
    """
    namespace = 'F'

    @property
    def all(self):
        result = []
        with self.pipe as pipe:
            ref = pipe.zrange(self._key, 0, -1)

            def cb():
                for v in ref.result:
                    result.append(v)

            pipe.on_execute(cb)

        return result


class User(Model):
    _fields = {
        'first_name': TextField,
        'last_name': TextField,
        'email': TextField,
        'beta_user': BooleanField,
        'admin': BooleanField,
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

    @property
    def email(self):
        return self._data['email']


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
    u.save(pipe=pipe)
    return u


if __name__ == '__main__':
    user = User(
        '1',
        first_name='John',
        last_name='Loehrer',
        email='72squared@gmail.com',
        beta_user=True)

    user.save(admin=True)

    with PipelineContext() as pipe:
        users = [User('1', pipe=pipe), User('2', pipe=pipe)]

        print("list of users before execute: %s" % users)

    print("list of users after execute: %s" % users)

    user = User('1')
    print("user 1: %s" % dict(user))

    user.delete()
    print("user 1 after delete: %s" % dict(user))

    with PipelineContext() as pipe:

        # create a bunch of test users
        users = [test_user(k, pipe=pipe) for k in range(1, 3)]

        # get them all from the database
        users = [User(u.key, pipe=pipe) for u in users]

        # create a list of followers for user 1
        f = Followers('1', pipe=pipe)

        # add some followers by id
        for n in range(2, 5):
            f.add("%s" % n, time.time())

        # get all of the followers we added
        result = f.all

        # now we run all of this in one big pipeline statement.
        # so far, nothing has hit the database.
        # Amazing, right?
        print("list of users before execute: %s" % result)

    print("list of users after execute: %s" % result)

    # print out the list of the generated users
    print("test users after execute:")

    for u in users:
        print("    %s" % dict(u))

    print("followers: %s" % result)

    # example of talking to two databases.
    with PipelineContext() as primary:
        with PipelineContext(name='alt') as alt:
            key = '123'
            first = primary.incr(key)
            second = alt.incr(key)

    print(first.result)
    print(second.result)
