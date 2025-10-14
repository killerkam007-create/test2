def test(a):
    if a<=40:
        return "Fail"
    elif a>40 and a<=60:
        return "Pass"
    elif a>60 and a<=80:
        return "Merit"
    else:
        return "Distinction"
    
def test_init_upper(a):
    b=list(a)
    b[0]=b[0].upper()
    return ''.join(b)

