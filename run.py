from hashlib import new
from lark import Lark
from lark import Transformer, Tree
from lark import Visitor

from berkeleydb import db
import sys
import re
import os


sys.ps1 = "DB_2017-15892> "
def checkIfDuplicates(listOfElems):
    ''' Check if given list contains any duplicates '''
    if len(listOfElems) == len(set(listOfElems)):
        return False
    else:
        return True

def numOfColumns(tableName):
    myDb = db.DB()
    myDb.open('./database/' + tableName)
    counter = 1
    cursor = myDb.cursor()
    while(cursor.next()) :
        result = myDb.get(str(counter).encode())
        if result is not None :
            counter +=1
    return(counter-1)

def typeChecking(queryColumnList,valueList, tableName) :
    myDB= db.DB()
    myDB.open('./database/' + tableName)
    result = []
    if (queryColumnList):
        for x in range(1, len(valueList) + 1) :
            column = myDB.get(str(x).encode())
            typeOfColumn = myDB.get(column) 
            typeOfColumn = typeOfColumn.decode()
            toCompare = valueList[x-1]
            for y in range(0, len(queryColumnList)):
                if column.decode() == queryColumnList[y].children[0]:
                    toCompare = valueList[y]
                    if (toCompare.isnumeric()):
                # if the value inserted is a number
                        if('int' in typeOfColumn):
                            # check if the schema is also a number
                            result.append(toCompare)
                            continue
                        else : 
                            # not a number so error. wrong type
                            print("Insertion has failed: Types are not matched") 
                            return
                        # the value inserted is not a number
                    else : 
                        temp = nullCheck(queryColumnList, valueList, tableName)
                        if temp :
                            return
                        if('int' in typeOfColumn):
                            print("Insertion has failed: Types are not matched") 
                            return
                        constraint = re.findall(r'\d+', typeOfColumn)
                        if len(toCompare) > int(constraint[0]) :
                            toCompare = toCompare[:int(constraint[0])]
                            result.append(toCompare)
                        else :
                            result.append(toCompare)
    else :
        for x in range(1, len(valueList) + 1) :
            column = myDB.get(str(x).encode())
            typeOfColumn = myDB.get(column) 
            typeOfColumn = typeOfColumn.decode()
            toCompare = valueList[x-1]
            if (toCompare.isnumeric()):
            # if the value inserted is a number
                if('int' in typeOfColumn):
                    # check if the schema is also a number
                    result.append(toCompare)
                    continue
                else : 
                    # not a number so error. wrong type
                    print("Insertion has failed: Types are not matched") 
                    return
                # the value inserted is not a number
            else : 
                temp = nullCheck(queryColumnList, valueList, tableName)
                if temp :
                    return
                if('int' in typeOfColumn):
                    print("Insertion has failed: Types are not matched") 
                    return
                constraint = re.findall(r'\d+', typeOfColumn)
                if len(toCompare) > int(constraint[0]) :
                    toCompare = toCompare[:int(constraint[0])]
                    result.append(toCompare)
                else :
                    result.append(toCompare)
    return result

def wrongColumnChecking(columnList, tableName):
    myDB= db.DB()
    myDB.open('./database/' + tableName)
    columnSchema = []
    for x in range(0, len(columnList)) :
        columnName = myDB.get(str(x+1).encode())
        if(columnName is not None):
            columnName = columnName.decode()
            columnSchema.append(columnName)
    for x in range(0, len(columnList)):
        if columnList[x].children[0] not in columnSchema:
            value = columnList[x].children[0]
            print(f'Insertion has failed: `[{value}]` does not exist')
            return
        else:
            continue
        
def nullCheck(queryColumnList, valueList, tableName):
    myDB= db.DB()
    myDB.open('./database/' + tableName)
    flag = False
    for x in range(0, len(valueList)):
        columnName = myDB.get(str(x+1).encode())
        constraint = myDB.get(columnName)
        primaryKey = myDB.get(b"primary key")  
        foreignKey = myDB.get(b"foreign key")
        if foreignKey is not None:
            foreignKey = foreignKey.decode()
        else :
            foreignKey = " "
        if primaryKey is not None:
            primaryKey = primaryKey.decode()
        else :
            primaryKey = ""
        if queryColumnList:
            for y in range(0, len(valueList)):
                if columnName.decode() == queryColumnList[y].children[0]:
                    if valueList[y] == 'null':
                        if('null' in constraint.decode() or queryColumnList[y].children[0] in primaryKey):
                            value = queryColumnList[y].children[0]
                            print(f"Insertion has failed: `[{value}]` is not nullable")
                            flag = True
                        else :
                            continue
        else :
            if valueList[x] == 'null':
                if('null' in constraint.decode() or columnName.decode() in primaryKey):
                    value = columnName.decode()
                    print(f"Insertion has failed: `[{value}]` is not nullable")
                    flag = True
                else :
                    continue
    return flag 

def recordInsert(queryColumnList, valueList, tableName):  
    valueList = typeChecking(queryColumnList, valueList, tableName)
    myDB = db.DB()
    myDB.open('./database/' + tableName)
    pkList = [] #key of record
    valuesList = [] #data of record
    foreignKey = myDB.get(b"foreign key")
    if foreignKey is not None:
        foreignKey = foreignKey.decode()
    else :
        foreignKey = " "
    primaryKey = myDB.get(b"primary key")  
    if primaryKey is not None:
        primaryKey = primaryKey.decode()
    else :
        primaryKey = ""
    if queryColumnList :
        for x in range(0, len(queryColumnList)):
            # for each of the columns in schema
            columnName = myDB.get(str(x+1).encode())
            columnName = columnName.decode()
            for y in range(0, len(queryColumnList)):
                # find the corresponding value 
                if columnName == queryColumnList[y].children[0]:
                    value = valueList[y]
                    if queryColumnList[y].children[0] in primaryKey:
                        pkList.append(value)
                    else :
                        valuesList.append(value)
    else :
        for x in range(0, len(valueList)):
            columnName = myDB.get(str(x+1).encode())
            columnName = columnName.decode()
            if columnName in primaryKey :
                pkList.append(valueList[x])
            else :
                valuesList.append(valueList[x])
     #foreignkey constraint
    fkList = []
    if queryColumnList :
        for x in range(0, len(queryColumnList)):
            columnName = myDB.get(str(x+1).encode())
            columnName = columnName.decode()
            if columnName in foreignKey :
                for y in range(0, len(queryColumnList)):
                    if columnName == queryColumnList[y].children[0]:
                        fkList.append(valueList[y])
        #all of the values of foreign key inside fkList
        #now check if the values in fkList is in the values of the referenced table
        findingFK = ''
        for x in range(0, len(fkList)):
            findingFK = findingFK + fkList[x] + ', '
        referencedTable = myDB.get(b"references")
        if referencedTable is not None :
            referencedTable = referencedTable.decode().split("(")[0]
            findDB = db.DB()
            findDB.open('./database/' + referencedTable)
            res = findDB.get(str(findingFK).encode())
            if res is None:
                print("Insertion has failed: Referential integrity violation")
                return
    else :
        for x in range(0, len(valueList)):
            columnName = myDB.get(str(x+1).encode())
            columnName = columnName.decode()
            if columnName in foreignKey :
                fkList.append(valueList[x])
        findingFK = ''
        for x in range(0, len(fkList)):
            findingFK = findingFK + fkList[x] + ', '
        referencedTable = myDB.get(b"references")
        if referencedTable is not None:
            referencedTable = referencedTable.decode().split("(")[0]
            findDB = db.DB()
            findDB.open('./database/' + referencedTable)
            res = findDB.get(str(findingFK).encode())
            if res is None:
                print("Insertion has failed: Referential integrity violation")
                return
    #now check if there are primary key value duplicates already in the database. 
    if not pkList :
        toAdd = ''
        for x in range(0, len(valuesList)):
            toAdd += valuesList[x] + ", "
        toAdd = toAdd.encode()
        # for tables with no primary key, the record itself is a primary key. save it as key data pair
        if myDB.get(toAdd) is not None :
            print("Insertion has failed: Primary key duplication")
            return
        else :
            myDB.put(toAdd, toAdd)
            print("The row is inserted")
    else :
        toAdd = ''
        toAddData = ''
        for x in range(0, len(pkList)):
            toAdd += pkList[x] + ", "
        for x in range(0, len(valuesList)):
            toAddData += valuesList[x] + ", "
        toAdd = toAdd.encode()
        toAddData = toAddData.encode()
        if myDB.get(toAdd) is not None :
            print("Insertion has failed: Primary key duplication")
            return
        else :
            myDB.put(toAdd, toAddData)
            print("The row is inserted")
def compParser(tree):
    columns = []
    comp_op = []
    operand = []
    table = []                                    #y
    #print(tree[3].children[1].children[0].children[2].children[1].children[0].children[0].children[0].children[0])
    if len(tree[3].children[1].children[0].children) < 3 :
        columns.append(tree[3].children[1].children[0].children[0].children[1].children[0].children[0].children[0].children[1].children[0])
        comp_op.append(tree[3].children[1].children[0].children[0].children[1].children[0].children[0].children[1])
        operand.append(tree[3].children[1].children[0].children[0].children[1].children[0].children[0].children[2].children[0].children[0])
        if(tree[3].children[1].children[0].children[0].children[1].children[0].children[0].children[0].children[0] is not None):
            table.append(tree[3].children[1].children[0].children[0].children[1].children[0].children[0].children[0].children[0].children[0])
        else :
            table.append('None')
    else :
        for x in range(0, len(tree[3].children[1].children[0].children),2) :
            columns.append(tree[3].children[1].children[0].children[x].children[1].children[0].children[0].children[0].children[1].children[0])
        for y in range(0, len(tree[3].children[1].children[0].children),2) :
            comp_op.append(tree[3].children[1].children[0].children[y].children[1].children[0].children[0].children[1])
        for z in range(0, len(tree[3].children[1].children[0].children),2) :
            operand.append(tree[3].children[1].children[0].children[z].children[1].children[0].children[0].children[2].children[0].children[0]) 
        for i in range(0, len(tree[3].children[1].children[0].children),2) :
            if(tree[3].children[1].children[0].children[i].children[1].children[0].children[0].children[0].children[0] is not None):
                table.append(tree[3].children[1].children[0].children[i].children[1].children[0].children[0].children[0].children[0].children[0])
            else :
                table.append("None")
    result = [columns, comp_op, operand, table]
    return result   
def typeChecker(listOfData, tableName):
    dir_path ='./database/'
    files = []
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            files.append(path)
    myDB = db.DB()
    myDB.open('./database/' + tableName)
    column = listOfData[0]
    returnValue = False
    for x in range (0, len(column)):
        if myDB.get(str(column[x]).encode()) is None :
            print("Where clause try to reference non existing column")
            returnValue = True
            return returnValue
    for x in range (0, len(column)):
        #for each column
        if listOfData[3][x] == "None":
            constraint = myDB.get(str(column[x]).encode())
            if constraint is not None :
                if 'int' in constraint.decode() :
                    if listOfData[2][x].isnumeric() :
                        continue
                    else :
                        print("Where clause try to compare incomparable values")
                        returnValue = True
                else :
                    #characters
                    if listOfData[2][x].isnumeric():
                        print("Where clause try to compare incomparable values")
                        returnValue = True
                    else:
                        continue
            else :
                print("Where clause try to reference non existing column")
                returnValue = True
        else :
            # go to the table to find the schema
            findDB = db.DB()
            if listOfData[3][x] in files :
                findDB.open('./database/' + listOfData[3][x])
            else :
                print("Where clause try to reference tables which are not specified")
                returnValue = True
                return returnValue
            constraint = findDB.get(str(column[x]).encode())
            if 'int' in constraint.decode() :
                if listOfData[2][x].isnumeric() :
                    continue
                else :
                    print("Where clause try to compare incomparable values")
                    returnValue = True
            else :
                #characters
                if listOfData[2][x].isnumeric():
                    print("Where clause try to compare incomparable values")
                    returnValue = True
                else:
                    continue
    return returnValue

def deleteQuery(parsedData, tableName):
    rowCounter = 0
    myDB = db.DB()
    myDB.open('./database/' + tableName)
    colnumbers = []
    cursor = myDB.cursor()
    while x := cursor.next() :
        if x[1].decode() in parsedData[0] and x[0].decode() != 'primary key' and x[0].decode() != 'foreign key' :
            colnumbers.append(x[0].decode())
    # get both key and data of the record in number order
    # compare the col depending on the number of colnumbers
    cursor2 = myDB.cursor()
    record = ""
    comparedValue = []
    while x := cursor2.next():
        key = x[0].decode()
        data = x[1].decode()
        if 'int' in data or 'char' in data or 'primary key' in key or 'referenced' in data or 'references' in key or 'foreign key' in key:
            continue
        elif myDB.get(x[1]) is not None :
            continue
        else :
            record = x[0].decode() + x[1].decode()
        comparedValue = record.split(",")
        comparedValue = comparedValue[:-1]
        resultByRecord = False
        for z in range(0, len(colnumbers)): # for each of the condition in where
            index = 100
            attribute = myDB.get(str(colnumbers[z]).encode()) # got the attribute from schema.
            for y in range(0, len(colnumbers)):
                if parsedData[0][y] == attribute.decode():
                    index = y
            ValuetoBeCompared = comparedValue[int(colnumbers[z])-1]
            ValuetoBeCompared = ValuetoBeCompared.split() # single record
            operator = parsedData[1][index]
            operand = parsedData[2][index]
            #separate evaluation for string and number
            if operand.isnumeric() and ValuetoBeCompared[0].isnumeric():
                if operator == '=' :
                    expression = str(ValuetoBeCompared[0]) + '==' + str(operand)
                    resultByRecord = eval(expression)
                else :
                    expression = str(ValuetoBeCompared[0]) + str(operator) + str(operand)
                    resultByRecord = eval(expression)
            else : 
                if operator == '=' :
                    operand= operand[1:-1]
                    resultByRecord = ValuetoBeCompared[0] == operand
            if not resultByRecord :  
                break
    
        if resultByRecord :
            myDB.delete(x[0]) 
            rowCounter += 1
        else :
            continue 
    if rowCounter == 0 :
        print("Where clause contains ambiguous reference")   
    else :
        print(f"{rowCounter} row(s) are deleted")
        
class executeQuery(Transformer):
    def select_query(self, tree):
        myDB = db.DB()
        myDB.open
        
    def insert_query(self, tree):
        dir_path ='./database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path)
        tableName = tree[2].children[0]
        if(tableName not in files):
            print("No such table")
            return
        #checking if table exist is done#
        #now get the values from the parser
        valueList = []
        myDB = db.DB()
        myDB.open('./database/' + tableName, dbtype = db.DB_HASH)
        queryColumnList = []
        if tree[3] is not None :
            if (len(tree[3].children[0].children)-2 != numOfColumns(tableName)):
                for x in range(1, len(tree[3].children[0].children)-1) :
                    queryColumnList.append(tree[3].children[0].children[x])
                wrongColumnChecking(queryColumnList, tableName)
                return
            if(len(tree[3].children[0].children) != len(tree[5].children)):
                #column in the query does not equal the values given in the query
                print("Insertion has failed: Types are not matched48") 
                return
            for x in range(1, len(tree[3].children[0].children)-1) :
                queryColumnList.append(tree[3].children[0].children[x])
        else :
            if (len(tree[5].children)-2 != numOfColumns(tableName)):
                print("Insertion has failed: Types are not matched") 
                return
        for x in range(1, len(tree[5].children)-1):
            columnData = tree[5].children[x].children[1]
            theColumn = myDB.get(str(x).encode())
            valueList.append(columnData)    
        #all the values of the record are inside the valueList in order
        #now onto type checking the integrity constraints
        if queryColumnList :
            wrongColumnChecking(queryColumnList, tableName)
        typeChecking(queryColumnList, valueList, tableName)
        recordInsert(queryColumnList, valueList, tableName)
       
    def delete_query(self, tree):
        dir_path ='./database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path)
        tableName = tree[2].children[0]
        if(tableName not in files):
            print("No such table")
            return
        myDB = db.DB()
        myDB.open('./database/' + tableName)
        cursor = myDB.cursor()
        globCounter = 0
        # if there is no where clause
        if tree[3] is None:
            #delete all records
            while x := cursor.next():
                key = x[0].decode()
                data = x[1].decode()
                if 'int' in data or 'char' in data or 'primary key' in key or 'referenced' in data or 'references' in key or 'foreign key' in key:
                    continue
                elif myDB.get(x[1]) is not None :
                    continue
                else :
                    myDB.delete(x[0])    
                    globCounter +=1
            print(f"{globCounter} row(s) are deleted")
            return
        parsedData = compParser(tree)
        retVal = typeChecker(parsedData, tableName)
        if retVal :
            return
        deleteQuery(parsedData, tableName)
        
    def update_query(self, tree):
        print("DB_2017-15892> 'UPDATE' requested")
        
    def show_table_query(self, tree):
        dir_path ='./database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path)
        print("-------------------")
        for file in files :
            print(file)
        print("-------------------")
    def desc_query(self,tree):
        dir_path ='./database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path)
        tableName = tree[1].children[0]
        if(tableName not in files):
            print("No such table")
            return
        myDB = db.DB()
        myDB.open("./database/" + tableName, dbtype = db.DB_HASH)
        cursor = myDB.cursor()
        print("---------------------------------------------------------")
        print(f"table_name [{tableName}]")
        column_name = "column_name"
        type = "type"
        null = "null"
        key ="key"
        line = column_name.ljust(20) + type.ljust(15) + null.ljust(15) + key.ljust(15)
        print(line)
        foreignKeys = ""
        primaryKeys = ""
        while x:= cursor.next():
            key = x[0]
            if (key.decode() == "foreign key"):
                res = myDB.get(key).decode()
                foreignKeys += res
            elif (key.decode() == "primary key"):
                res = myDB.get(key).decode()
                primaryKeys += res
            else: 
                continue
        cursor2 = myDB.cursor()
        while x:= cursor2.next():
            key = x[0]
            if (key.decode() == "foreign key"):
                continue
            elif (key.decode() == "primary key"):
                continue
            elif (key.decode() == "references"):
                continue
            elif(key.decode() in files):
                continue
            elif(key.decode().isnumeric()):
                continue
            else :
                #need to filter records
                column_name = key.decode()
                if (', ' in column_name):
                    continue
                else :
                    if("not null" in myDB.get(key).decode()):
                        const = myDB.get(key).decode()
                        const = const[ :len(const) - 8]
                        type = const
                        null = "N"
                    else :
                        type = myDB.get(key).decode()
                        null = "Y"
                    if(column_name in primaryKeys and column_name in foreignKeys):
                        key = "PRI/FOR"
                        null = "N"
                    elif(column_name in primaryKeys):
                        key = "PRI"
                        null = "N"
                    elif(column_name in foreignKeys):
                        key ="FOR"
                    else:
                        key=" "  
            line2 = column_name.ljust(20) + type.ljust(15) + null.ljust(15) + key.ljust(15)
            print(line2)
        print("---------------------------------------------------------")   
    def drop_table_query(self,tree):
        tableName= tree[2].children[0]
        #check if tableName exists in db
        dir_path = './database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path)
        if(tableName not in files):
            print("No such table")
            return
        myDB= db.DB()
        myDB.open("./database/" + tableName, dbtype = db.DB_HASH)
        cursor = myDB.cursor()
        while x:=cursor.next():
            key = x[0]
            if(myDB.get(key).decode() == "referenced"):
                print(f"Drop table has failed: \'{tableName}\' is referenced by other table")
                return
            #before we remove it, we need to take the referenced schema out of the table it references
        refTable = myDB.get(b"references")
        if(refTable != None):
            refTable = refTable.decode()
            refTable = refTable[:refTable.index("(")]
            tempDB= db.DB()
            tempDB.open('./database/' + refTable, dbtype=db.DB_HASH)
            tempName = tableName.encode()
            tempDB.delete(tempName)
        os.remove('./database/'+ tableName)
        print(f"\'{tableName}\' table is dropped")
    def create_table_query(self,tree):
        tableName =tree[2].children[0]
        list =[]
        primaryKeyList = []
        primaryKeyColumns = []
        #check database for table with existing table name
        #-----------------------------------------------------------------------------------------
        #iterate over files in database directory. Get the name of the file which is the table name, and if it is equal, print error
        dir_path = './database/'
        files = []
        for path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, path)):
                files.append(path.lower())
        for file in files:
            if (tableName.lower() == file):
                print("Create table has failed: table with the same name already exists")
                return
        #-----------------------------------------------------------------------------------------
        #check for duplicate columns and multiple primary key definitions and conditions
        for x in range(1, len(tree[3].children)-1): 
            column = tree[3].children[x].children[0].children[0].children[0]
            if(column.lower() != 'primary'):
                list.append(column.lower())
            else:
                #primary key conditions
                primaryKeyList.append(column)
                for y in range(1, len(tree[3].children[x].children[0].children[0].children[2].children)-1):
                    primaryKey = tree[3].children[x].children[0].children[0].children[2].children[y].children[0]
                    primaryKeyColumns.append(primaryKey)
                duplicate = checkIfDuplicates(primaryKeyColumns)
                #composite keys in primaryKeyColumns
                if(duplicate):
                    print("Create table has failed: primary key definition is duplicated")
                    return
                compositePrimaryKey = ""
                for z in range (0,len(primaryKeyColumns)):
                    if(z == len(primaryKeyColumns)-1):
                        compositePrimaryKey += primaryKeyColumns[z]
                    else :
                        compositePrimaryKey += primaryKeyColumns[z] + ", "
        duplicate = checkIfDuplicates(list)
        #check for multiple lines of primary key declarations
        primaryKeyDuplicate = checkIfDuplicates(primaryKeyList)
        if(duplicate):
            print("Create table has failed: column definition is duplicated")
            return
        if(primaryKeyDuplicate):
            print("Create table has failed: primary key definition is duplicated")
            return
        #now check if the column exists
        doesNotExist = False
        for x in range (0, len(primaryKeyColumns)):
            if (primaryKeyColumns[x].lower() not in list):
                print(f"Create table has failed: \'{primaryKeyColumns[x]}\' does not exists in column definition")
                doesNotExist = True   
        #exit out if error
        if(doesNotExist):
            return
        #checking for duplicate columns and multiple primary key definitions done if program has not returned until here, it means all the conditions
        #for primary key and duplicate columns is false
        #-----------------------------------------------------------------------------------------
        #onto foreign key checking now
        list2 = []
        constraintList= {}
        foreignKeyList= []
        referencingColumnList= []
        for i in range(1, len(tree[3].children)-1): 
            temp = ''
            column = tree[3].children[i].children[0].children[0].children[0]
            if(column != 'foreign' and column != 'primary'):
                #normal columns
                list2.append(column)
                for tokens in range(0,len(tree[3].children[i].children[0].children[1].children)):
                    temp += tree[3].children[i].children[0].children[1].children[tokens]
                constraintList[column] = temp
            elif (column == 'foreign'):
                #handle multiple foreignkeys
                if(len(tree[3].children[i].children[0].children[0].children[2].children) != len(tree[3].children[i].children[0].children[0].children[5].children)):
                    print("Create table has failed: foreign key references non primary key column")
                    print("Create table has failed: foreign key references wrong type")
                    return
                for j in range(1, len(tree[3].children[i].children[0].children[0].children[2].children)-1):
                    foreignKeyList.append(tree[3].children[i].children[0].children[0].children[2].children[j].children[0])
                    referencingColumnList.append(tree[3].children[i].children[0].children[0].children[5].children[j].children[0])
                referencingTable = tree[3].children[i].children[0].children[0].children[4].children[0]
                #check if the referencd table exists
                if(referencingTable.lower() not in files):
                    print("Create table has failed: foreign key references non existing table")
                    return
                theDB2 = db.DB()
                theDB2.open("./database/" + referencingTable, dbtype = db.DB_HASH)
                checkIfPrimaryKey2 = theDB2.get(b"primary key")
                if(checkIfPrimaryKey2):
                    checkIfPrimaryKey2 = checkIfPrimaryKey2.decode()
                    numOfPKs2 = checkIfPrimaryKey2.split(",")
                else:
                    print("Create table has failed: foreign key references non primary key column")
                    return
                if(referencingTable.lower() == tableName.lower()):
                    print("Create table has failed: foreign key references non existing table")
                    return
                #the table exists
                if(checkIfDuplicates(foreignKeyList)):
                    print("Create table has failed: column definition is duplicated")
                    return
                if(len(tree[3].children[i].children[0].children[0].children[2].children) != len(tree[3].children[i].children[0].children[0].children[5].children)):
                    if(foreignKeyList != numOfPKs2):
                        print("Create table has failed: foreign key references non primary key column")
                        return
                    else:
                        print("Create table has failed: foreign key references wrong type")
                    #differnt number of column in foreign key and referenced columns error
                        return
                else:
                    #check if composite key is all there
                    wow = checkIfPrimaryKey2.split(",")
                    if(len(wow) == len(referencingColumnList)):
                        for w in referencingColumnList:
                            if(w.lower() not in checkIfPrimaryKey2.lower()):
                                print("Create table has failed: foreign key references non primary key column")
                                return  
                    else:
                        print("Create table has failed: foreign key references non primary key column")
                        return   
                for k in range(0,len(foreignKeyList)):
                    theDB = db.DB()
                    theDB.open("./database/" + referencingTable, dbtype = db.DB_HASH)
                    if(foreignKeyList[k] not in list2):
                        print(f"Create table has failed: \'{foreignKeyList[k]} \' does not exists in column definition")
                        return            
                    #all the foreign key is in the column definition
                    if((foreignKeyList[k] not in referencingColumnList)):
                        print("Create table has failed: foreign key references wrong type")
                        return
                    schemaKey = foreignKeyList[k].encode()
                    schemaConstraint = theDB.get(schemaKey)
                    fromDB = schemaConstraint.decode()
                    fromThisTable = constraintList.get(schemaKey.decode())
                    if(fromThisTable == 'int'):
                        if(fromThisTable not in fromDB):
                            print("Create table has failed: foreign key references wrong type")
                            return
                    else:
                        if(fromThisTable not in fromDB):
                            print("Create table has failed: foreign key references wrong type")
                            return
                    #checking for type of schema done
                    checkIfPrimaryKey = theDB.get(b"primary key")
                    checkIfPrimaryKey = checkIfPrimaryKey.decode()
                    if(foreignKeyList[k] not in checkIfPrimaryKey):
                        print("Create table has failed: foreign key references non primary key column")
                        return
                    theDB.close()
                    theDB2.close()
                #now onto checking the foreign key with the referenced table.   
        #-----------------------------------------------------------------------------------------
        #all error checking is done until here. Will add the schema to the database from here
        newTable = db.DB()
        newTable.open('./database/'+tableName, dbtype= db.DB_HASH, flags=db.DB_CREATE)
        pkList = ""
        keynumber = 1
        for x in range(1, len(tree[3].children)-1): 
            column = tree[3].children[x].children[0].children[0].children[0]
            fkList =""
            rcList = ""
            if(column == "primary"):
                for keys in range(1, len(tree[3].children[x].children[0].children[0].children[2].children)-1):
                    pk = tree[3].children[x].children[0].children[0].children[2].children[keys].children[0] + ", "
                    pkList += pk
                pkList = pkList[:len(pkList) -2]
                pkList = pkList.encode()
                newTable.put(b"primary key", pkList)
            elif(column == "foreign"):
                for j in range(1, len(tree[3].children[x].children[0].children[0].children[2].children)-1):
                    fkList += (tree[3].children[x].children[0].children[0].children[2].children[j].children[0])+ ", "
                    rcList += (tree[3].children[x].children[0].children[0].children[5].children[j].children[0]) + ", "
                referencingTable = tree[3].children[x].children[0].children[0].children[4].children[0]
                fkList = fkList[:len(fkList) - 2]
                rcList = rcList[:len(rcList) - 2]
                fkList = fkList.encode()
                refDB = db.DB()
                refDB.open("./database/" + referencingTable, dbtype= db.DB_HASH)
                refTable = tableName.encode()
                refDB.put(refTable, b"referenced")
                refDB.close()
                newTable.put(b"foreign key", fkList)
                refTableAndrcList = referencingTable + "(" + rcList + ")"
                refTableAndrcList = refTableAndrcList.encode()
                newTable.put(b"references", refTableAndrcList)
            else :
                constraint = tree[3].children[x].children[0].children[1:]
                if(constraint[2] != None):
                    # columns with not null constraint
                    #check for char size less than 1
                    (val1, val2, val3) = constraint
                    temp=""
                    for x in range(len(val1.children)):
                        temp += val1.children[x]
                    if(temp.find("char")!= -1):
                        wow = int(re.search(r'\d+', temp).group())
                        if(wow < 1):
                            print("Char length should be over 0")
                            os.remove('./database/' + tableName)
                            return
                    result = temp + " " + val2 + " " + val3
                else :
                    # columns with no 'not null' constraint
                    (val1, val2, val3 ) = constraint
                    result = ""
                    for x in range(len(val1.children)):
                        result += val1.children[x]
                    if(result.find("char")!= -1):
                        wow = int(re.search(r'\d+', result).group())
                        if(wow < 1):
                            print("Char length should be over 0")
                            os.remove('./database/' + tableName)
                            return
                column = column.encode()
                result = result.encode()
                numkey = str(keynumber)
                numkey = numkey.encode()
                newTable.put(column, result) 
                newTable.put(numkey, column) # order of attributes is now saved. Use this number for insert
                keynumber += 1     
    #----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    #all checking for error is done. Now create the table in data base and add the schema
        print(f"\'{tableName}\' table is created")
        newTable.close()
        null = lambda self, _: None
        true = lambda self, _: True
        false = lambda self, _: False
        def EXIT(self, tree):
            exit()
            
def printAllTables():
    dir_path = './database/'
    files = []
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            files.append(path)
    for file in files:
        newDB = db.DB()
        newDB.open('./database/' + file, dbtype=db.DB_HASH)
        cursor = newDB.cursor()
        while x := cursor.next():
            print(x)
        print()
        
def main():
    sys.ps1 = "DB_2017-15892> "
    with open("grammar.lark") as file:
        sql_parser = Lark(file.read(), start="command", parser="earley")
    while True:
        userInput = input("DB_2017-15892> ")
        queries = userInput.split(";")
        queries = filter(None, queries)
        for query in queries:
            query = query + ';'
            query = query.replace("\\n", "")
            query = query.replace("\\r", "")
            if (query == "exit;"):
                exit()
            else:
                try:
                    entry = sql_parser.parse(query)
                    executeQuery(visit_tokens=True).transform(entry)
                    printAllTables()
                except Exception as e:
                    print("DB_2017-15892> Syntax error")
                    print(e)
                    break


main()
