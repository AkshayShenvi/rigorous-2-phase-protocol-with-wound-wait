import pandas as pd

timestamp = 0
lock_table = pd.DataFrame(columns=["transaction_id", "data_item", "state"])
transaction_table = pd.DataFrame(columns=["transaction_id", "timestamp", "state"])
wait_list = []

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

    return input_ops;


def begin(transaction_id):
    global timestamp
    global transaction_table
    global lock_table

    print("Transaction " + str(transaction_id) + ": start")

    # increment transaction timestamp
    timestamp = timestamp + 1

    # add transaction to transaction table
    row = {"transaction_id": transaction_id, "timestamp": timestamp, "state": "active"}
    transaction_table = transaction_table.append(row, ignore_index=True)


def read(transaction_id, data_item):

    global timestamp
    global transaction_table
    global lock_table
    global wait_list

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        
        # exclusive lock by another transaction, wound wait
        if len(lock_table[(lock_table["data_item"] == data_item) 
                & (lock_table["transaction_id"] != transaction_id)]) > 0:
            
            print("Transaction " + str(transaction_id) + ": requesting write lock on " + data_item + ": wound wait")

            # get conflicting lock
            conflicting_lock = lock_table[(lock_table["transaction_id"] != transaction_id) & (lock_table["data_item"] == data_item)]

            # get conflcting transaction
            conflicting_transaction_id = conflicting_lock["transaction_id"].values[0]

            wound_wait(transaction_id, conflicting_transaction_id)
        
        # exclusive lock by self, read
        elif len(lock_table[(lock_table["data_item"] == data_item) 
            & (lock_table["transaction_id"] == transaction_id) 
            & (lock_table["state"] == 'write')]) > 0:
            
            print("Transaction " + str(transaction_id) + ": read " + data_item)
        
        # shared lock by self, read
        elif len(lock_table[(lock_table["data_item"] == data_item) 
            & (lock_table["transaction_id"] == transaction_id) 
            & (lock_table["state"] == "read")]) > 0:
            
            print("Transaction " + str(transaction_id) + ": read " + data_item)
        
        # no exclusive lock, okay to lock and read
        else:
            
            print("Transaction " + str(transaction_id) + ": acquire read lock on " + data_item)

            # add lock to lock table
            lock = {"transaction_id": transaction_id, "data_item": data_item, "state": "read"}
            lock_table = lock_table.append(lock, ignore_index=True)

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        print("Transaction " + transaction_id + " is waiting, added operation to waiting list")
        wait_list.append(('read', transaction_id, data_item))
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        print("Transaction " + str(transaction_id) + " is aborted")
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        print("Transaction " + str(transaction_id) + " is completed")

    # current transaction has invalid state
    else:
        print("Transaction " + str(transaction_id) + " state is invalid")


def write(transaction_id, data_item):

    global timestamp
    global transaction_table
    global lock_table
    global wait_list

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        
        # lock by another transaction, wound wait
        if len(lock_table[(lock_table["data_item"] == data_item) 
                & (lock_table["transaction_id"] != transaction_id)]) > 0:
            
            print("Transaction " + str(transaction_id) + ": requesting write lock on " + data_item + ": wound wait")

            # get conflicting lock
            conflicting_lock = lock_table[(lock_table["transaction_id"] != transaction_id) & (lock_table["data_item"] == data_item)]

            # get conflcting transaction
            conflicting_transaction_id = conflicting_lock["transaction_id"].values[0]

            wound_wait(transaction_id, conflicting_transaction_id)
            

        # exclusive lock by self, write
        elif len(lock_table[(lock_table["data_item"] == data_item)
                & (lock_table["transaction_id"] == transaction_id)
                & (lock_table["state"] == "write")]) > 0:
            
            print("Transaction " + str(transaction_id) + ": write " + data_item)
        
        # no exclusive lock, okay to lock and write
        else:

            print("Transaction " + str(transaction_id) + ": acquire write lock on " + data_item)

            # 
            lock_index = lock_table[(lock_table["transaction_id"] == transaction_id) & (lock_table["data_item"] == data_item)].index
            lock_table.at[lock_index, "state"] = "write"

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        print("Transaction " + str(transaction_id) + " is waiting, added operation to waiting list")

        # add to wait_list
        wait_list.append(('write', transaction_id, data_item))
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        print("Transaction " + str(transaction_id) + " is aborted")
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        print("Transaction " + str(transaction_id) + " is completed")

    # current transaction has invalid state
    else:
        print("Transaction " + str(transaction_id) + " state is invalid")


def end(transaction_id):
    global timestamp
    global transaction_table
    global lock_table

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        print("Transaction " + str(transaction_id) + ": end")
        
        # release all locks held by the transaction
        locks_to_release = lock_table[lock_table["transaction_id"] == transaction_id]
        
        # print("locks to release: " + str(locks_to_release))
        lock_table = lock_table[lock_table["transaction_id"] != transaction_id]

        # change status of transaction to completed
        transaction_index = transaction_table[transaction_table["transaction_id"] == transaction_id].index
        transaction_table.at[transaction_index, 'state'] = "completed"

        run_wait_list()

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        print("Transaction " + str(transaction_id) + " is waiting, added operation to waiting list")

        # add to wait_list
        wait_list.append(('end', transaction_id))
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        print("Transaction " + str(transaction_id) + " is aborted")
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        print("Transaction " + str(transaction_id) + " is completed")

    # current transaction has invalid state
    else:
        print("Transaction " + str(transaction_id) + " state is invalid")


def wound_wait(requesting_transaction_id, locking_transaction_id):

    # get transactions having lock
    requesting_transaction = transaction_table[transaction_table["transaction_id"] == requesting_transaction_id]
    locking_transaction = transaction_table[transaction_table["transaction_id"] == locking_transaction_id]
    # print("Locking Transaction: " + str(locking_transaction_id))

    locking_timestamp = transaction_table[transaction_table["transaction_id"] == locking_transaction_id]["timestamp"].values[0]
    requesting_timestamp = transaction_table[transaction_table["transaction_id"] == requesting_transaction_id]["timestamp"].values[0]

    if (requesting_timestamp > locking_timestamp):
        print('wait requesting transaction ' + str(requesting_timestamp))

        # set status of requesting transaction to 'waiting'
        requesting_transaction_index = requesting_transaction.index
        transaction_table.at[requesting_transaction_index, "state"] = "waiting"

    else:
        print('abort locking transaction ' + str(locking_timestamp))

        # set status of locking transaction to 'aborted'
        locking_transaction_index = locking_transaction.index
        transaction_table.at[requesting_transaction_index, "state"] = "aborted"

        # release locks
        lock_table = lock_table[lock_table["transaction_id"] != locking_transaction_id]

        # check if wait-listed operations can be executed
        run_wait_list()


def run_wait_list():
    print('----------------------------------------')
    print('Start waitlisted operations:')
    # loop through wait_list
    for op in wait_list:
        if op[0] == 'begin':
            begin(int(op[1]))
        elif op[0] == 'read':
            read(int(op[1]), op[2])
        elif op[0] == 'write':
            write(int(op[1]), op[2])
        elif op[0] == 'end':
            end(int(op[1]))
    print('End waitlisted operations:')
    print('----------------------------------------')

if __name__=='__main__':
    file=open("input2.txt","r+")
    input=file.read()
    input_ops = checkchar(input)

    for op in input_ops:
        op = checkoperation(op)

        if op[0] == 'begin':
            begin(int(op[1]))
        elif op[0] == 'read':
            read(int(op[1]), op[2])
        elif op[0] == 'write':
            write(int(op[1]), op[2])
        elif op[0] == 'end':
            end(int(op[1]))
    
    print()        
    print("transaction_table: ")
    print(transaction_table)
    
    print()
    print("lock_table: ")
    print(lock_table)
    
    print()
    print("wait_list: ")
    print(wait_list)
    
    # # print( transaction_table[(transaction_table["transaction_id"] > 1) & (transaction_table["timestamp"] < 3)] )
    # print('detete transaction 1')
    # print(transaction_table[transaction_table["transaction_id"] == 1].index)
    # print(transaction_table)

    file.close()

