import queue
transaction_table = {}
locks_table = {}
counter=0
waiting_list=[]


def check_operation(opr):

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

def clean_input(input):
    input_ops=input.split("\n")
    operation=[]

    for n,i in enumerate(input_ops):
        temp_clean=i.replace(" ","")
        input_ops[n]=temp_clean
        operation.append(check_operation(i))
    return operation

def read(item,t_id):
    if item in locks_table:
        lock_info=locks_table[item]
        if t_id in lock_info['t_id_waiting']:
            # waiting_list.append('r'+)

        else:


    else:
        new_item={}
        new_item['state']='read'
        new_item['t_id_holding']=[]
        new_item['t_id_waiting']=[]
        new_item['t_id_holding'].append(t_id)
        locks_table[item]=new_item
        

def write(item,t_id):

def begin(transaction_id,counter):
    new_transaction = {}

    new_transaction['timestamp'] = counter
    new_transaction['state'] = 'active'
    new_transaction['locked_items'] = []

    transaction_table[transaction_id]=new_transaction




if __name__=='__main__':
    operation_list=[]

    file=open("input.txt","r+")
    input=file.read()
    operation_list=clean_input(input)
    print(operation_list)
    for op in operation_list:
        if op[0] == 'begin':
            counter+=1
            begin(op[1],counter)
            print(transaction_table)
            print(locks_table)
        if op[0] == 'read':
            read(op[2],op[1])
            print(transaction_table)
            print(locks_table)
        if op[0] == 'end':
            #check holding locks
            #try to execute waiting_lock of that item on which ended transaction had a holding lock.
            #release all locks
            print("end")
    file.close()

