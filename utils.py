from threading import Thread


def runThread(func):
    thread = Thread(name=f"Func_{func}", target=func, daemon=True)
    thread.start()


def runThread1Arg(func, arg1):
    thread = Thread(name=f"Func_{func}_{arg1}", target=func, args=(arg1,), daemon=True)
    thread.start()
