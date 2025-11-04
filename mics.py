#reverse String 
def reverse_string(s):
    b=a.split(" ")
    print(b)
    j=[]
    for item in range(len(b)-1,-1,-1):
        j.append(b[item])
    k=" ".join(j)
    print(k)
#################factorial of a number recursively
def factorial_test(n):
    if n==0 or n==1:
        return 1
    else:
        return n*factorial_test(n-1)
#################find duplicate value in a list
    
def duplicateValue(a):
    b=set(a)
    dup=[]
    dup_dict={}
    for item in b:
        if a.count(item)>1:
            dup.append(item)
            dup_dict[item]=a.count(item)
    print(dup)
    print(f'duplicate value and its count is {dup_dict}')


####Remove duplicates from a list while preserving order.
def preservingOrderList(a):
    c=[]
    for item in a:
        if item not in c:
            c.append(item)
        
    print(f'list preserving order is {c}')
#####flatten a nested list
def flatten_lsit(a):
    b=[]
    for item in a:
        if isinstance(item,list):
            b.extend(item)
        else:
            b.append(item)
            
    print (f'Flatten list is {b}')