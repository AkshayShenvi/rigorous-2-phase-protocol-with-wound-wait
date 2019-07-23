def checkoperation(opr):
    operation = 'invalid'
    transaction_num = 'invalid'
    item=None
    if opr[0]=='b':
        operation='begin'
        transaction_num=opr[1]
    elif opr[0]=='r':
        operation='read'
        transaction_num = opr[1]
        item=opr[3]
    elif opr[0]=='w':
        operation='write'
        transaction_num = opr[1]
        item = opr[3]
    elif opr[0]=='e':
        operation='end'
        transaction_num = opr[1]

    return operation,transaction_num,item
def checkchar(input):
    input_ops=input.split("\n")
    operation=[]

    for n,i in enumerate(input_ops):
        temp_clean=i.replace(" ","")
        input_ops[n]=temp_clean


    for i in input_ops:
        operation.append(checkoperation(i))
    print(operation)




if __name__=='__main__':
    file=open("input.txt","r+")
    input=file.read()
    checkchar(input)
    file.close()

