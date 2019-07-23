def checkoperation(opr):
    if opr=='b':
        operation='begin'
    elif opr=='r':
        operation='read'
    elif opr=='w':
        operation='write'
    elif opr=='e':
        operation='end'
    else:
        operation='invalid operation'
    return operation
def checkchar(input):
    operations=input.split("\n")
    operation=[]
    for i in operations:
        operation.append(checkoperation(i[0]))
    print(operation)




if __name__=='__main__':
    file=open("input.txt","r+")
    input=file.read()
    checkchar(input)
    file.close()

