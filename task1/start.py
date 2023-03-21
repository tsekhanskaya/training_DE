import logging

from task1.classes.main import Main


try:
    main = Main()
    main.work()
except Exception as e:
    logging.exception(e)
