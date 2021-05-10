using Chain

foo(x, y) = x * y

x = [1, 2]

@chain x begin
    identity
    identity
    identity
    foo([3, 4])
end