l = [1, 2]
l.append(3)
print(l)   # [1, 2, 3]

l = [1, 2]
l.extend([3, 4])
print(l)   # [1, 2, 3, 4]

l = [1, 3]
l.insert(1, 2)
print(l)   # [1, 2, 3]

l = [1, 2, 2, 3]
l.remove(2)
print(l)   # [1, 2, 3]

l = [1, 2, 3]
l.pop()
print(l)   # [1, 2]

l = [1, 2, 3]
l.clear()
print(l)   # []