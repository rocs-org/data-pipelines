from returns.pipeline import pipe


def hello_world():
    return "hello world"


if __name__ == "__main__":
    pipe(hello_world, print)()
