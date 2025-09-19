def x():
    i = 0
    while True:
        yield i
        i += 1


nextid = x()
print(next(nextid))
print(next(nextid))
