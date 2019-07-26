import pandas as pd

input_file = 'input.txt'

timestamp = 0
lock_table = pd.DataFrame(columns=["transaction_id", "data_item", "state"])
transaction_table = pd.DataFrame(columns=["transaction_id", "timestamp", "state"])
wait_list = []
output_list=[]

in_waitlist = False

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
    global output_list

    # append to output file
    output = "b" + str(transaction_id) + ";     :" + "Transaction " + str(transaction_id) + ": start"
    if in_waitlist:
        output = output + ' (from waitlist)'
    print(output)
    output_list.append(output)

    # increment transaction timestamp
    timestamp = timestamp + 1

    # add transaction to transaction table
    row = {"transaction_id": transaction_id, "timestamp": timestamp, "state": "active"}
    transaction_table = transaction_table.append(row, ignore_index=True)
    return True

def read(transaction_id, data_item):

    global timestamp
    global transaction_table
    global lock_table
    global wait_list
    global output_list
    global in_waitlist

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        
        # exclusive lock by another transaction, wound wait
        if len(lock_table[(lock_table["data_item"] == data_item) 
                & (lock_table["transaction_id"] != transaction_id)]) > 0:

            # get conflicting lock
            conflicting_lock = lock_table[(lock_table["transaction_id"] != transaction_id) & (lock_table["data_item"] == data_item)]

            # get conflcting transaction
            conflicting_transaction_id = conflicting_lock["transaction_id"].values[0]

            # wound wait
            wound_wait_result = wound_wait(transaction_id, conflicting_transaction_id)

            # apply read lock to requesting transaction if locking transaction is aborted
            message = ""
            if wound_wait_result == 'abort':

                # release all locks held by the transaction
                locks_to_release = str(lock_table[lock_table["transaction_id"] == conflicting_transaction_id]["data_item"].values)[1:-1]
                lock_table = lock_table[lock_table["transaction_id"] != conflicting_transaction_id]

                # apply read lock
                lock = {"transaction_id": transaction_id, "data_item": data_item, "state": "read"}
                lock_table = lock_table.append(lock, ignore_index=True)

                # set message string
                message = "Abort Transaction " + str(conflicting_transaction_id) + " and release locks on " + locks_to_release + ", acquire read lock on " + data_item
            else:

                # add blocked operation to wait_list
                wait_list.append(("read", transaction_id, data_item))

                # set message string
                message = "Block Transaction " + str(transaction_id) + ", add operation to wait list"

            # append to output file
            output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": Requesting read lock on " + data_item + " : wound wait : " + message
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            # run waitlisted transactions if aborted
            if wound_wait_result == 'abort':
                run_wait_list()

            return False

        
        # exclusive lock by self, read
        elif len(lock_table[(lock_table["data_item"] == data_item) 
            & (lock_table["transaction_id"] == transaction_id) 
            & (lock_table["state"] == 'write')]) > 0:

            # append to output file
            output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": read " + data_item
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            return True
        
        # shared lock by self, read
        elif len(lock_table[(lock_table["data_item"] == data_item) 
            & (lock_table["transaction_id"] == transaction_id) 
            & (lock_table["state"] == "read")]) > 0:

            # append to output file
            output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": read " + data_item
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            return True
        
        # no exclusive lock, okay to lock and read
        else:

            # append to output file
            output = "r" + str(transaction_id) + "(" + data_item + ");  :" + "Transaction " + str(transaction_id) + ": acquire read lock on " + data_item
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            # add lock to lock table
            lock = {"transaction_id": transaction_id, "data_item": data_item, "state": "read"}
            lock_table = lock_table.append(lock, ignore_index=True)

            return True

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        
        # append to output file
        output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": waiting, added operation to waiting list"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        # add operation to wait_list
        wait_list.append(('read', transaction_id, data_item))

        return False
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        
        # append to output file
        output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " is aborted"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        
        # append to output file
        output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " is completed"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True

    # current transaction has invalid state
    else:

        # append to output file
        output = "r" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " state is invalid"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True


def write(transaction_id, data_item):

    global timestamp
    global transaction_table
    global lock_table
    global wait_list
    global output_list
    global in_waitlist

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        
        # lock by another transaction, wound wait
        if len(lock_table[(lock_table["data_item"] == data_item) 
                & (lock_table["transaction_id"] != transaction_id)]) > 0:

            # get conflicting lock
            conflicting_lock = lock_table[(lock_table["transaction_id"] != transaction_id) & (lock_table["data_item"] == data_item)]

            # get conflcting transaction
            conflicting_transaction_id = conflicting_lock["transaction_id"].values[0]

            wound_wait_result = wound_wait(transaction_id, conflicting_transaction_id)

            # apply read lock to requesting transaction if locking transaction is aborted
            message = ""
            if wound_wait_result == 'abort':

                # release all locks held by the transaction
                locks_to_release = str(lock_table[lock_table["transaction_id"] == conflicting_transaction_id]["data_item"].values)[1:-1]
                lock_table = lock_table[lock_table["transaction_id"] != conflicting_transaction_id]

                # apply write lock
                lock_index = lock_table[(lock_table["transaction_id"] == transaction_id) & (lock_table["data_item"] == data_item)].index
                if len(lock_index.values) == 0:
                    # dirty write
                    lock = {"transaction_id": transaction_id, "data_item": data_item, "state": "write"}
                    lock_table = lock_table.append(lock, ignore_index=True)
                else:
                    lock_table.at[lock_index, "state"] = "write"

                # set message string
                message = "Abort Transaction " + str(conflicting_transaction_id) + " and release locks on " + locks_to_release + ", acquire write lock on " + data_item
            else:
                
                # add blocked operation to wait_list
                wait_list.append(("read", transaction_id, data_item))

                # set message string
                message = "Block Transaction " + str(transaction_id) + ", add operation to wait list"

            # append to output file
            output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": Requesting write lock on " + data_item + ": wound wait : " + message
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            # run waitlisted transactions if aborted
            if wound_wait_result == 'abort':
                run_wait_list()

            return False

        # exclusive lock by self, write
        elif len(lock_table[(lock_table["data_item"] == data_item)
                & (lock_table["transaction_id"] == transaction_id)
                & (lock_table["state"] == "write")]) > 0:
            
            # append to output file
            output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": write " + data_item
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            return True
        
        # no exclusive lock, okay to lock and write
        else:

            # append to output file
            output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": acquire write lock on " + data_item
            if in_waitlist:
                output = output + ' (from waitlist)'
            print(output)
            output_list.append(output)

            # apply write lock
            lock_index = lock_table[(lock_table["transaction_id"] == transaction_id) & (lock_table["data_item"] == data_item)].index
            if len(lock_index.values) == 0:
                # dirty write
                lock = {"transaction_id": transaction_id, "data_item": data_item, "state": "write"}
                lock_table = lock_table.append(lock, ignore_index=True)
            else:
                lock_table.at[lock_index, "state"] = "write"

            return True

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        
        # append to output file
        output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + ": waiting, added operation to waiting list"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        # add to wait_list
        wait_list.append(('write', transaction_id, data_item))

        return False
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        
        # append to output file
        output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " is aborted"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        
        # append to output file
        output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " is completed"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True

    # current transaction has invalid state
    else:

        # append to output file
        output = "w" + str(transaction_id) + "(" + data_item + ");  :Transaction " + str(transaction_id) + " state is invalid"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True


def end(transaction_id):
    global timestamp
    global transaction_table
    global lock_table
    global output_list

    # current transaction is active
    if transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "active":
        
        # release all locks held by the transaction
        locks_to_release = str(lock_table[lock_table["transaction_id"] == transaction_id]["data_item"].values)[1:-1]
        lock_table = lock_table[lock_table["transaction_id"] != transaction_id]

        # append to output file
        output="e" + str(transaction_id) + ";     :Transaction " + str(transaction_id) + ": end, release locks on " + locks_to_release
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        # change status of transaction to completed
        transaction_index = transaction_table[transaction_table["transaction_id"] == transaction_id].index
        transaction_table.at[transaction_index, 'state'] = "completed"

        run_wait_list()

        return True

    # current transaction is waiting
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "waiting":
        
        # append to output file
        output = "e" + str(transaction_id) + ";     :Transaction " + str(transaction_id) + ": waiting, added operation to waiting list"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        # add to wait_list
        wait_list.append(('end', transaction_id))

        return False
    
    # current transaction is aborted
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "aborted":
        
        # append to output file
        output = "e" + str(transaction_id) + ";     :Transaction " + str(transaction_id) + " is aborted"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True
    
    # current transaction is completed
    elif transaction_table[transaction_table["transaction_id"] == transaction_id]["state"].values[0] == "completed":
        
        # append to output file
        output = "e" + str(transaction_id) + ";  :Transaction " + str(transaction_id) + " is completed"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True

    # current transaction has invalid state
    else:

        # append to output file
        output = "e" + str(transaction_id) + ";  :Transaction " + str(transaction_id) + " state is invalid"
        if in_waitlist:
            output = output + ' (from waitlist)'
        print(output)
        output_list.append(output)

        return True


def wound_wait(requesting_transaction_id, locking_transaction_id):

    global transaction_table
    global lock_table

    # get transactions having lock
    requesting_transaction = transaction_table[transaction_table["transaction_id"] == requesting_transaction_id]
    locking_transaction = transaction_table[transaction_table["transaction_id"] == locking_transaction_id]
    # print("Locking Transaction: " + str(locking_transaction_id))

    locking_timestamp = transaction_table[transaction_table["transaction_id"] == locking_transaction_id]["timestamp"].values[0]
    requesting_timestamp = transaction_table[transaction_table["transaction_id"] == requesting_transaction_id]["timestamp"].values[0]

    if (requesting_timestamp > locking_timestamp):
        output = 'wait'

        # set status of requesting transaction to 'waiting'
        requesting_transaction_index = requesting_transaction.index
        transaction_table.at[requesting_transaction_index, "state"] = "waiting"

    else:
        output = 'abort'

        # set status of locking transaction to 'aborted'
        locking_transaction_index = locking_transaction.index
        transaction_table.at[locking_transaction_index, "state"] = "aborted"

    return output


def run_wait_list():
    global transaction_table
    global wait_list
    global in_waitlist

    # return if wait list operations already being executed, or if waitlist is already empty
    if in_waitlist or len(wait_list) == 0:
        return

    in_waitlist = True

    # print('----------------------------------------')
    # print('Start waitlisted operations:')
    # loop through wait_list

    while True:

        # break if wait_list is empty
        if len(wait_list) == 0:
            break

        op = wait_list[0]
        wait_list = wait_list[1:]

        transaction_index = transaction_table[transaction_table["transaction_id"] == int(op[1])].index
        
        # check if waitlisted operation is not aborted
        if transaction_table.iloc[transaction_index]["state"].values[0] != 'aborted':
            transaction_table.at[transaction_index, "state"] = "active"
        else:
            continue

        if op[0] == 'begin':
            if not begin(int(op[1])):
                break
        elif op[0] == 'read':
            if not read(int(op[1]), op[2]):
                break
        elif op[0] == 'write':
            if not write(int(op[1]), op[2]):
                break
        elif op[0] == 'end':
            if not end(int(op[1])):
                break

    in_waitlist = False

    # print('End waitlisted operations:')
    # print('----------------------------------------')

if __name__=='__main__':
    output_list
    file=open(input_file, "r+")
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

    # execute any operations if remaining
    run_wait_list()
    
    print()        
    print("transaction_table: ")
    print(transaction_table)
    
    print()
    print("lock_table: ")
    print(lock_table)
    
    print()
    print("wait_list: ")
    print(wait_list)
    #lines=[]

    #print((input_ops))
    #print((output_list))
    with open('output.txt', 'w') as f:
        for item in output_list:
            f.write("%s\n" % item)

    file1=open('output.txt',"a+")
    file1.write("\n*number of output operations may be greater than number of input operations \n because operations that are waiting are started once \n the locks are acquired")
    file1.close()
    # for i in range(len(output_list)):
    #     lines.append(input_ops[i]+":"+output_list[i])
    # final_string = '\n'.join(lines)
    # print(final_string)
    # with open('output.txt', 'w') as f:
    #     for item in lines:
    #         f.write("%s\n" % item)



    # # print( transaction_table[(transaction_table["transaction_id"] > 1) & (transaction_table["timestamp"] < 3)] )
    # print('detete transaction 1')
    # print(transaction_table[transaction_table["transaction_id"] == 1].index)
    # print(transaction_table)

    file.close()

