import os
import shutil
import numpy as np


def cwd_cleanup(func):
    def inner(*args):
        while True:
            directory = "dir" + str(np.random.randint(10000, 99999))
            try:
                os.mkdir(directory)
            except FileExistsError:
                continue
            break
        os.chdir(directory)
        try:
            return_value = func(*args)
        except Exception as error:
            raise error
        finally:
            os.chdir("../")
            shutil.rmtree(directory)
        return return_value

    return inner
