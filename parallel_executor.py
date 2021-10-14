"""
if you need to have some operations done by a separate thread,
to keep the main thread focused on the execution, you can do that using many ways,
but if you also prefer to use just one thread to do the Ops,
then you can use this ParallelExecutor class to create an instance of a Thread that will do that for you

Author: Mohamed El-Hasnaouy
Telegram: t.me/elhasnaouymed
"""


from threading import Thread
from typing import Optional, Callable, Tuple, Dict, Deque, Any, TypeVar
from collections import deque
from time import sleep


class ParallelExecutor(Thread):
    _Method = Tuple[Callable, Optional[Tuple], Optional[Dict], Callable]
    _Exception = TypeVar('_Exception')
    _ERROR_CALLBACK = Callable[[Callable, Tuple, Dict, _Exception], Any]

    def __init__(self, delay: Optional[float] = 0.0, error_callback: Optional[_ERROR_CALLBACK] = None):
        """
        This class allows you to create a separate thread to execute your methods parallelly

        :param delay: the time to do time.sleep after every execution
        :param error_callback: to call when any error is raised with the method

        when any error with the result_callback, no option will be done

        Example:

        e = ParallelExecutor(0)

        e.append(save_to_db)

        e.start()

        e.end()
        """
        # error checking
        if error_callback is not None and not callable(error_callback):
            raise ValueError('_error_callback must be a Callable or just None')
        #
        Thread.__init__(self)  # inherit the attributes
        Method = self._Method  # define the method constant
        self._methods: Deque[Method] = deque()  # create the methods holder
        self._delay = delay  # setting the delay
        self._error_callback = error_callback
        self._end = False  # default value to False
        self._terminate = False  # default value to False
        self._running = False  # default value to False

    def run(self, auto_end=False):
        """
        if you execute this method, the orders will be running in current thread
        you should use start() to create and run in new thread
        :return:
        """
        self._end = auto_end
        self._terminate = False
        self._running = True
        while self._terminate or not(self._end and len(self._methods) == 0):
            # < if _terminate is True exit
            # < or if the _end is True and there are no methods on list exit
            try:  # to avoid any error that can break the loop
                if len(self._methods) > 0:  # when there is any method on list
                    method, args, kwargs, result_call_back = self._methods[0]  # get the method info
                    del self._methods[0]  # delete it
                    #
                    try:
                        result = method(*args, **kwargs)  # execute the method and take the result
                    except Exception as e:  # when any error is happen with the method
                        if self._error_callback is not None:  # when there is an error_callback
                            self._error_callback(method, args, kwargs, e)  # callback
                    else:  # when no error is raised
                        if callable(result_call_back) and result is not None:  # callback for result if required
                            result_call_back(result)
            except Exception:
                pass
            finally:  # execute anyway
                sleep(self._delay)  # wait as delay says
        self._running = False

    def is_running(self):
        return bool(self._running)

    def append(self, method: Callable, args: Optional[Tuple] = None, kwargs: Optional[Dict] = None, result_callback: Optional[Callable] = None):
        """
        to append a new method to list for execution, it will be added to the end of the list
        :param method: Callable method
        :param args: its args, if any exists
        :param kwargs: its kwargs, if any exists
        :param result_callback: a method to call when a result is returned by an executed method (Optional)
        :return: index where appended
        """
        if self._end:
            raise PermissionError('You are not allowed to add methods after ending the loop!')
        args = tuple() if args is None else args  # to make sure that is a tuple
        kwargs = dict() if kwargs is None else kwargs  # to make sure that is a dict
        self._methods.append((method, args, kwargs, result_callback))

    def end(self):
        """
        to end the loop after executing all methods,
        when called, you are not allowed to put any new methods into list
        :return: None
        """
        self._end = True

    def terminate(self):
        """
        to terminate the thread instantaneously, without executing the rest of methods in list
        :return:
        """
        self._terminate = True
