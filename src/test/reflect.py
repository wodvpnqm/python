#encoding UTF8
def map(iterable,func):
    target =  []
    for item in iterable:
        target.append(func(item))
    return target

def addOne(item):
    return item + 1

list = [1,2,3]
tuple = (1,2,3)
print list
print map(list,lambda x:x+1)