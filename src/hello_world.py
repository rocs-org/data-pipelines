from returns.pipeline import pipe


def hello_world():
    return "Hello World!"


if __name__ == "__main__":
    pipe(hello_world, print)()
