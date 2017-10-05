import os
import re
import subprocess
from contextlib import contextmanager

from fabric.operations import _AttributeString, _prefix_commands, _prefix_env_vars
from fabric.state import env as _env
from fabric.state import output, win32


class CommandError(Exception):
    def __init__(self, out):
        super().__init__(out.stderr)
        self.out = out


def local(command, capture=False, shell=None, ignore_errors=False, env=None):
    """
    Run a command on the local system.
    `local` is simply a convenience wrapper around the use of the builtin
    Python ``subprocess`` module with ``shell=True`` activated. If you need to
    do anything special, consider using the ``subprocess`` module directly.
    ``shell`` is passed directly to `subprocess.Popen
    <http://docs.python.org/library/subprocess.html#subprocess.Popen>`_'s
    ``execute`` argument (which determines the local shell to use.)  As per the
    linked documentation, on Unix the default behavior is to use ``/bin/sh``,
    so this option is useful for setting that value to e.g.  ``/bin/bash``.
    `local` is not currently capable of simultaneously printing and
    capturing output, as `~fabric.operations.run`/`~fabric.operations.sudo`
    do. The ``capture`` kwarg allows you to switch between printing and
    capturing as necessary, and defaults to ``False``.
    When ``capture=False``, the local subprocess' stdout and stderr streams are
    hooked up directly to your terminal, though you may use the global
    :doc:`output controls </usage/output_controls>` ``output.stdout`` and
    ``output.stderr`` to hide one or both if desired. In this mode, the return
    value's stdout/stderr values are always empty.
    When ``capture=True``, you will not see any output from the subprocess in
    your terminal, but the return value will contain the captured
    stdout/stderr.
    In either case, as with `~fabric.operations.run` and
    `~fabric.operations.sudo`, this return value exhibits the ``return_code``,
    ``stderr``, ``failed``, ``succeeded``, ``command`` and ``real_command``
    attributes. See `run` for details.
    `~fabric.operations.local` will honor the `~fabric.context_managers.lcd`
    context manager, allowing you to control its current working directory
    independently of the remote end (which honors
    `~fabric.context_managers.cd`).
    .. versionchanged:: 1.0
        Added the ``succeeded`` and ``stderr`` attributes.
    .. versionchanged:: 1.0
        Now honors the `~fabric.context_managers.lcd` context manager.
    .. versionchanged:: 1.0
        Changed the default value of ``capture`` from ``True`` to ``False``.
    .. versionadded:: 1.9
        The return value attributes ``.command`` and ``.real_command``.
    """
    given_command = command
    # Apply cd(), path() etc
    with_env = _prefix_env_vars(command, local=True)
    wrapped_command = _prefix_commands(with_env, 'local')
    # if output.debug:
    #     print("[localhost] local: %s" % (wrapped_command))
    # elif output.running:
    #     print("[localhost] local: " + given_command)
    # Tie in to global output controls as best we can; our capture argument
    # takes precedence over the output settings.
    dev_null = None
    if capture:
        out_stream = subprocess.PIPE
        err_stream = subprocess.PIPE
    else:
        dev_null = open(os.devnull, 'w+')
        # Non-captured, hidden streams are discarded.
        out_stream = None if output.stdout else dev_null
        err_stream = None if output.stderr else dev_null
    if env is None:
        env = os.environ
    try:
        cmd_arg = wrapped_command if win32 else [wrapped_command]
        p = subprocess.Popen(cmd_arg, shell=True, stdout=out_stream,
                             stderr=err_stream, executable=shell,
                             close_fds=(not win32),
                             env=env)
        (stdout, stderr) = p.communicate()
    finally:
        if dev_null is not None:
            dev_null.close()
    # Handle error condition (deal with stdout being None, too)
    out = _AttributeString(stdout.decode().strip() if stdout else "")
    err = _AttributeString(stderr.decode().strip() if stderr else "")
    out.command = given_command
    out.real_command = wrapped_command
    out.failed = False
    out.return_code = p.returncode
    out.stderr = err
    if p.returncode not in _env.ok_ret_codes and not ignore_errors:
        out.failed = True
        # msg = "local() encountered an error (return code %s) while executing '%s'" % (p.returncode, command)
        # print('\n\n')
        # if out:
        #     print('{}'.format(out))
        # print('\n')
        # if err:
        #     print('{}'.format(err))
        # sys.exit(1)
        raise CommandError(out)
        # error(message=msg, stdout=out, stderr=err)
    out.succeeded = not out.failed
    # If we were capturing, this will be a string; otherwise it will be None.
    return out


run = local


@contextmanager
def already_exists(expr):
    try:
        yield
    except Exception as e:
        if not re.search(expr, str(e)):
            raise

# from fabric.api import task, run as fabrun


# @task
# def run(cmd):
#     fabrun(cmd)


# from contextlib import contextmanager

# # from fabric.operations import _run_command
# from fabric.context_managers import (
#     warn_only as warn_only_manager,
#     quiet as quiet_manager
# )


# @contextmanager
# def _noop():
#     yield


# env = {
#     'use_shell': False,

# }


# def _shell_wrap(command, shell_escape, shell=True, sudo_prefix=None):
#     """
#     Conditionally wrap given command in env.shell (while honoring sudo.)
#     """
#     # Honor env.shell, while allowing the 'shell' kwarg to override it (at
#     # least in terms of turning it off.)
#     if shell and not env.use_shell:
#         shell = False
#     # Sudo plus space, or empty string
#     if sudo_prefix is None:
#         sudo_prefix = ""
#     else:
#         sudo_prefix += " "
#     # If we're shell wrapping, prefix shell and space. Next, escape the command
#     # if requested, and then quote it. Otherwise, empty string.
#     if shell:
#         shell = env.shell + " "
#         if shell_escape:
#             command = _shell_escape(command)
#         command = '"%s"' % command
#     else:
#         shell = ""
#     # Resulting string should now have correct formatting
#     return sudo_prefix + shell + command


# def _run_command(command, shell=True, pty=True, combine_stderr=True,
#                  sudo=False, user=None, quiet=False, warn_only=False,
#                  stdout=None, stderr=None, group=None, timeout=None,
#                  shell_escape=None, capture_buffer_size=None):
#     """
#     Underpinnings of `run` and `sudo`. See their docstrings for more info.
#     """
#     manager = _noop
#     if warn_only:
#         manager = warn_only_manager
#     # Quiet's behavior is a superset of warn_only's, so it wins.
#     if quiet:
#         manager = quiet_manager
#     with manager():
#         # Set up new var so original argument can be displayed verbatim later.
#         given_command = command

#         # # Check if shell_escape has been overridden in env
#         # if shell_escape is None:
#         #     shell_escape = env.get('shell_escape', True)

#         # Handle context manager modifications, and shell wrapping
#         wrapped_command = _shell_wrap(
#             _prefix_env_vars(_prefix_commands(command, 'remote')),
#             shell_escape,
#             shell,
#             _sudo_prefix(user, group) if sudo else None
#         )
#         # Execute info line
#         which = 'sudo' if sudo else 'run'
#         if output.debug:
#             print("[%s] %s: %s" % (env.host_string, which, wrapped_command))
#         elif output.running:
#             print("[%s] %s: %s" % (env.host_string, which, given_command))

#         # Actual execution, stdin/stdout/stderr handling, and termination
#         result_stdout, result_stderr, status = _execute(
#             channel=default_channel(), command=wrapped_command, pty=pty,
#             combine_stderr=combine_stderr, invoke_shell=False, stdout=stdout,
#             stderr=stderr, timeout=timeout,
#             capture_buffer_size=capture_buffer_size)

#         # Assemble output string
#         out = _AttributeString(result_stdout)
#         err = _AttributeString(result_stderr)

#         # Error handling
#         out.failed = False
#         out.command = given_command
#         out.real_command = wrapped_command
#         if status not in env.ok_ret_codes:
#             out.failed = True
#             msg = "%s() received nonzero return code %s while executing" % (
#                 which, status
#             )
#             if env.warn_only:
#                 msg += " '%s'!" % given_command
#             else:
#                 msg += "!\n\nRequested: %s\nExecuted: %s" % (
#                     given_command, wrapped_command
#                 )
#             error(message=msg, stdout=out, stderr=err)

#         # Attach return code to output string so users who have set things to
#         # warn only, can inspect the error code.
#         out.return_code = status

#         # Convenience mirror of .failed
#         out.succeeded = not out.failed

#         # Attach stderr for anyone interested in that.
#         out.stderr = err

#         return out

# run = _run_command
