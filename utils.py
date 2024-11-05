from threading import Thread


def runThread(func):
    thread = Thread(target=func, daemon=True)
    thread.start()


def runThread1Arg(func, arg1):
    thread = Thread(target=func, args=(arg1,), daemon=True)
    thread.start()
