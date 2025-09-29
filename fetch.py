def x():
    for x in range(3):
        match x:
            case 0:
                z = 1
            case 1:
                print(z)
                z = 2
            case 2:
                print(z)
                z = 3


x()
