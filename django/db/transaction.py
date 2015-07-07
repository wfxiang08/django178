# -*- coding: utf-8 -
"""
This module implements a transaction manager that can be used to define
transaction handling in a request or view function. It is used by transaction
control middleware and decorators.

The transaction manager can be in managed or in auto state. Auto state means the
system is using a commit-on-save strategy (actually it's more like
commit-on-change). As soon as the .save() or .delete() (or related) methods are
called, a commit is made.

Managed transactions don't do those commits, but will need some kind of manual
or implicit commits or rollbacks.
"""

import warnings

from functools import wraps

from django.db import (DatabaseError, Error, ProgrammingError)
from django.utils.decorators import available_attrs
from django.utils.deprecation import RemovedInDjango18Warning


class TransactionManagementError(ProgrammingError):
    """
    This exception is thrown when transaction management is used improperly.
    """
    pass


################
# Private APIs #
################


###########################
# Deprecated private APIs #
###########################

def abort(connection):
    """
    Roll back any ongoing transactions and clean the transaction management
    state of the connection.

    This method is to be used only in cases where using balanced
    leave_transaction_management() calls isn't possible. For example after a
    request has finished, the transaction state isn't known, yet the connection
    must be cleaned up for the next request.
    """
    # 清空状态
    connection.abort()


def enter_transaction_management(connection, managed=True, forced=False):
    """
    Enters transaction management for a running thread. It must be balanced with
    the appropriate leave_transaction_management call, since the actual state is
    managed as a stack.

    The state and dirty flag are carried over from the surrounding block or
    from the settings, if there is no surrounding block (dirty is always false
    when no current block is running).
    """
    connection.enter_transaction_management(managed, forced)



def leave_transaction_management(connection):
    """
    Leaves transaction management for a running thread. A dirty flag is carried
    over to the surrounding block, as a commit will commit all changes, even
    those from outside. (Commits are on connection level.)
    """
    connection.leave_transaction_management()


def is_dirty(connection):
    """
    Returns True if the current transaction requires a commit for changes to
    happen.
    """
    connection.is_dirty()


def set_dirty(connection):
    """
    Sets a dirty flag for the current thread and code streak. This can be used
    to decide in a managed block of code to decide whether there are open
    changes waiting for commit.
    """
    connection.set_dirty()


def set_clean(connection):
    """
    Resets a dirty flag for the current thread and code streak. This can be used
    to decide in a managed block of code to decide whether a commit or rollback
    should happen.
    """
    connection.set_clean()


def is_managed(connection):
    warnings.warn("'is_managed' is deprecated.", RemovedInDjango18Warning, stacklevel=2)


def managed(flag=True, connection = None):
    warnings.warn("'managed' no longer serves a purpose.",RemovedInDjango18Warning, stacklevel=2)


def commit_unless_managed(connection):
    warnings.warn("'commit_unless_managed' is now a no-op.", RemovedInDjango18Warning, stacklevel=2)


def rollback_unless_managed(connection):
    warnings.warn("'rollback_unless_managed' is now a no-op.",RemovedInDjango18Warning, stacklevel=2)


###############
# Public APIs #
###############

def get_autocommit(connection):
    """
    Get the autocommit status of the connection.
    """
    connection.get_autocommit()


def set_autocommit(autocommit, connection):
    """
    Set the autocommit status of the connection.
    """
    # set_autocommit(True, connection)

    connection.set_autocommit(autocommit)


def commit(connection):
    """
    Commits a transaction and resets the dirty flag.
    """
    connection.commit()


def rollback(connection):
    """
    Rolls back a transaction and resets the dirty flag.
    """
    connection.rollback()


def savepoint(connection):
    """
    Creates a savepoint (if supported and required by the backend) inside the
    current transaction. Returns an identifier for the savepoint that will be
    used for the subsequent rollback or commit.
    """
    connection.savepoint()


def savepoint_rollback(sid, connection):
    """
    Rolls back the most recent savepoint (if one exists). Does nothing if
    savepoints are not supported.
    """
    connection.savepoint_rollback(sid)


def savepoint_commit(sid, connection):
    """
    Commits the most recent savepoint (if one exists). Does nothing if
    savepoints are not supported.
    """
    connection.savepoint_commit(sid)


def clean_savepoints(connection):
    """
    Resets the counter used to generate unique savepoint ids in this thread.
    """
    connection.clean_savepoints()


def get_rollback(connection):
    """
    Gets the "needs rollback" flag -- for *advanced use* only.
    """
    connection.get_rollback()


def set_rollback(rollback, connection):
    """
    Sets or unsets the "needs rollback" flag -- for *advanced use* only.

    When `rollback` is `True`, it triggers a rollback when exiting the
    innermost enclosing atomic block that has `savepoint=True` (that's the
    default). Use this to force a rollback without raising an exception.

    When `rollback` is `False`, it prevents such a rollback. Use this only
    after rolling back to a known-good state! Otherwise, you break the atomic
    block and data corruption may occur.
    """
    connection.set_rollback(rollback)


#################################
# Decorators / context managers #
#################################

# savepoints: http://hideto.iteye.com/blog/195275
class Atomic(object):
    """
    This class guarantees the atomic execution of a given block.

    An instance can be used either as a decorator or as a context manager.

    When it's used as a decorator, __call__ wraps the execution of the
    decorated function in the instance itself, used as a context manager.

    When it's used as a context manager, __enter__ creates a transaction or a
    savepoint, depending on whether a transaction is already in progress, and
    __exit__ commits the transaction or releases the savepoint on normal exit,
    and rolls back the transaction or to the savepoint on exceptions.

    It's possible to disable the creation of savepoints if the goal is to
    ensure that some code runs within a transaction without creating overhead.

    A stack of savepoints identifiers is maintained as an attribute of the
    connection. None denotes the absence of a savepoint.

    This allows reentrancy even if the same AtomicWrapper is reused. For
    example, it's possible to define `oa = @atomic('other')` and use `@oa` or
    `with oa:` multiple times.

    Since database connections are thread-local, this is thread-safe.
    """

    def __init__(self, connection, savepoint):
        self.savepoint = savepoint
        self.connection = connection

    def __enter__(self):
        connection = self.connection

        if not connection.in_atomic_block:
            # Reset state when entering an outermost atomic block.
            connection.commit_on_exit = True
            connection.needs_rollback = False
            if not connection.get_autocommit():
                # Some database adapters (namely sqlite3) don't handle
                # transactions and savepoints properly when autocommit is off.
                # Turning autocommit back on isn't an option; it would trigger
                # a premature commit. Give up if that happens.
                if connection.features.autocommits_when_autocommit_is_off:
                    raise TransactionManagementError(
                        "Your database backend doesn't behave properly when "
                        "autocommit is off. Turn it on before using 'atomic'.")
                # When entering an atomic block with autocommit turned off,
                # Django should only use savepoints and shouldn't commit.
                # This requires at least a savepoint for the outermost block.
                if not self.savepoint:
                    raise TransactionManagementError(
                        "The outermost 'atomic' block cannot use "
                        "savepoint = False when autocommit is off.")
                # Pretend we're already in an atomic block to bypass the code
                # that disables autocommit to enter a transaction, and make a
                # note to deal with this case in __exit__.
                connection.in_atomic_block = True
                connection.commit_on_exit = False

        if connection.in_atomic_block:
            # We're already in a transaction; create a savepoint, unless we
            # were told not to or we're already waiting for a rollback. The
            # second condition avoids creating useless savepoints and prevents
            # overwriting needs_rollback until the rollback is performed.
            if self.savepoint and not connection.needs_rollback:
                sid = connection.savepoint()
                connection.savepoint_ids.append(sid)
            else:
                connection.savepoint_ids.append(None)
        else:
            # We aren't in a transaction yet; create one.
            # The usual way to start a transaction is to turn autocommit off.
            # However, some database adapters (namely sqlite3) don't handle
            # transactions and savepoints properly when autocommit is off.
            # In such cases, start an explicit transaction instead, which has
            # the side-effect of disabling autocommit.
            if connection.features.autocommits_when_autocommit_is_off:
                connection._start_transaction_under_autocommit()
                connection.autocommit = False
            else:
                connection.set_autocommit(False)
            connection.in_atomic_block = True

    def __exit__(self, exc_type, exc_value, traceback):
        connection = self.connection

        if connection.savepoint_ids:
            sid = connection.savepoint_ids.pop()
        else:
            # Prematurely unset this flag to allow using commit or rollback.
            connection.in_atomic_block = False

        try:
            if connection.closed_in_transaction:
                # The database will perform a rollback by itself.
                # Wait until we exit the outermost block.
                pass

            elif exc_type is None and not connection.needs_rollback:
                if connection.in_atomic_block:
                    # Release savepoint if there is one
                    if sid is not None:
                        try:
                            connection.savepoint_commit(sid)
                        except DatabaseError:
                            try:
                                connection.savepoint_rollback(sid)
                            except Error:
                                # If rolling back to a savepoint fails, mark for
                                # rollback at a higher level and avoid shadowing
                                # the original exception.
                                connection.needs_rollback = True
                            raise
                else:
                    # Commit transaction
                    try:
                        connection.commit()
                    except DatabaseError:
                        try:
                            connection.rollback()
                        except Error:
                            # An error during rollback means that something
                            # went wrong with the connection. Drop it.
                            connection.close()
                        raise
            else:
                # This flag will be set to True again if there isn't a savepoint
                # allowing to perform the rollback at this level.
                connection.needs_rollback = False
                if connection.in_atomic_block:
                    # Roll back to savepoint if there is one, mark for rollback
                    # otherwise.
                    if sid is None:
                        connection.needs_rollback = True
                    else:
                        try:
                            connection.savepoint_rollback(sid)
                        except Error:
                            # If rolling back to a savepoint fails, mark for
                            # rollback at a higher level and avoid shadowing
                            # the original exception.
                            connection.needs_rollback = True
                else:
                    # Roll back transaction
                    try:
                        connection.rollback()
                    except Error:
                        # An error during rollback means that something
                        # went wrong with the connection. Drop it.
                        connection.close()

        finally:
            # Outermost block exit when autocommit was enabled.
            if not connection.in_atomic_block:
                if connection.closed_in_transaction:
                    connection.connection = None
                elif connection.features.autocommits_when_autocommit_is_off:
                    connection.autocommit = True
                else:
                    connection.set_autocommit(True)
            # Outermost block exit when autocommit was disabled.
            elif not connection.savepoint_ids and not connection.commit_on_exit:
                if connection.closed_in_transaction:
                    connection.connection = None
                else:
                    connection.in_atomic_block = False

    def __call__(self, func):
        @wraps(func, assigned=available_attrs(func))
        def inner(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return inner

# TODO:
def atomic(connection=None, savepoint=True):
    # Bare decorator: @atomic -- although the first argument is called
    # `using`, it's actually the function being decorated.

    # @atomic
    if callable(connection):
        # 在使用gevent时，connection是动态获取的，因此不能直接使用 @atomic
        assert False
        # return Atomic(DEFAULT_DB_ALIAS, savepoint)(using)
    # Decorator: @atomic(...) or context manager: with atomic(...): ...
    else:
        # with atomic(conneciton=xxx)
        return Atomic(connection, savepoint)



############################################
# Deprecated decorators / context managers #
############################################

class Transaction(object):
    """
    Acts as either a decorator, or a context manager.  If it's a decorator it
    takes a function and returns a wrapped function.  If it's a contextmanager
    it's used with the ``with`` statement.  In either event entering/exiting
    are called before and after, respectively, the function/block is executed.

    autocommit, commit_on_success, and commit_manually contain the
    implementations of entering and exiting.
    """
    def __init__(self, entering, exiting, connection):
        self.entering = entering
        self.exiting = exiting
        self.connection = connection

    def __enter__(self):
        self.entering(connection = self.connection)

    def __exit__(self, exc_type, exc_value, traceback):
        self.exiting(exc_type, connection = self.connection)

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return inner


def _transaction_func(entering, exiting, connection):
    """
    Takes 3 things, an entering function (what to do to start this block of
    transaction management), an exiting function (what to do to end it, on both
    success and failure, and using which can be: None, indicating using is
    DEFAULT_DB_ALIAS, a callable, indicating that using is DEFAULT_DB_ALIAS and
    to return the function already wrapped.

    Returns either a Transaction objects, which is both a decorator and a
    context manager, or a wrapped function, if using is a callable.
    """
    # Note that although the first argument is *called* `using`, it
    # may actually be a function; @autocommit and @autocommit('foo')
    # are both allowed forms.

    if callable(connection):
        assert False
    return Transaction(entering, exiting, connection)


def autocommit(connection):
    """
    Decorator that activates commit on save. This is Django's default behavior;
    this decorator is useful if you globally activated transaction management in
    your settings file and want the default behavior in some view functions.
    """
    warnings.warn("autocommit is deprecated in favor of set_autocommit.",
        RemovedInDjango18Warning, stacklevel=2)

    def entering(connection):
        enter_transaction_management(managed=False, connection=connection)

    def exiting(exc_type, connection):
        leave_transaction_management(connection)

    return _transaction_func(entering, exiting, connection)


def commit_on_success(connection):
    """
    This decorator activates commit on response. This way, if the view function
    runs successfully, a commit is made; if the viewfunc produces an exception,
    a rollback is made. This is one of the most common ways to do transaction
    control in Web apps.
    """
    warnings.warn("commit_on_success is deprecated in favor of atomic.", RemovedInDjango18Warning, stacklevel=2)

    def entering(connection):
        enter_transaction_management(connection)

    def exiting(exc_type, connection):
        try:
            if exc_type is not None:
                if is_dirty(connection):
                    rollback(connection)
            else:
                if is_dirty(connection):
                    try:
                        commit(connection)
                    except:
                        rollback(connection)
                        raise
        finally:
            leave_transaction_management(connection)

    return _transaction_func(entering, exiting, connection)


def commit_manually(connection):
    """
    Decorator that activates manual transaction control. It just disables
    automatic transaction control and doesn't do any commit/rollback of its
    own -- it's up to the user to call the commit and rollback functions
    themselves.
    """
    warnings.warn("commit_manually is deprecated in favor of set_autocommit.",
        RemovedInDjango18Warning, stacklevel=2)

    def entering(connection):
        enter_transaction_management(connection)

    def exiting(exc_type, connection):
        leave_transaction_management(connection)

    return _transaction_func(entering, exiting, connection)


def commit_on_success_unless_managed(connection, savepoint=False):
    """
    Transitory API to preserve backwards-compatibility while refactoring.

    Once the legacy transaction management is fully deprecated, this should
    simply be replaced by atomic. Until then, it's necessary to guarantee that
    a commit occurs on exit, which atomic doesn't do when it's nested.

    Unlike atomic, savepoint defaults to False because that's closer to the
    legacy behavior.
    """
    if connection.get_autocommit() or connection.in_atomic_block:
        return atomic(savepoint, connection)
    else:
        def entering(connection):
            pass

        def exiting(exc_type, connection):
            set_dirty(connection)

        return _transaction_func(entering, exiting, connection)
