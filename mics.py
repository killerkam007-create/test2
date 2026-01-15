#reverse String 
def reverse_string(a):
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


#####Find the nth largest number in a list
def nth_largest_number(a,n):
    if (n-1)<len(set(a)):
        b=list(set(a))
        b.sort(reverse=True)
        print(f'{n} largest element in a list {a} is {b[n-1]}')
    else:
        print(f'{n} is out of index')
def count_frequency(list1,value):
    count=0
    for item in list1:
        if item==value:
            count+=1
    return count

def reverse_mics(l1):
    l2=[]
    for item in range(len(l1)-1,-1,-1):
        l2.append(l1[item])
    return l2


    