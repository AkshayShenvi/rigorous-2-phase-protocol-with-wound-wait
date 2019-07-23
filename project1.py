transaction_list = []
locks_dict = {}

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

    for n, i in enumerate(input_ops):
        temp_clean = i.replace(" ","")
        input_ops[n] = temp_clean

    for i in input_ops:
        operation.append(checkoperation(i))
    print(operation)

    for op in operation:
        if op[0] == 'begin':
            begin(op[1])

def begin(transaction_id):
    new_transaction = {}
    new_transaction['transaction_id'] = transaction_id
    new_transaction['timestamp'] = 0
    new_transaction['state'] = 'active'
    new_transaction['item_list'] = []

    transaction_list.append(new_transaction)

    print('new transaction started: ' + str(new_transaction))


if __name__=='__main__':
    file=open("input.txt","r+")
    input=file.read()
    checkchar(input)
    file.close()

