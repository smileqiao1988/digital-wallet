
# coding: utf-8

# In[1]:

import sys


# In[2]:

import Queue


# In[48]:

class Customer:
    
    def __init__(self, name, left=None, right=None, parent=None, color='W', degree=0):
        self.degree = degree
        self.color = color
        self.name = name
        #Container to store 1st degree nodes for this customer
        self.adjacentree = Adjacentree()   

    #add node to the adjacentree
    def add_adjacentree_node(self, node):
        if not self.adjacentree.find(node.name):
            self.adjacentree.add_node(node) 
    '''
    #shallow copy node, with only adjacentree in deep copy 
    def smart_copy(self): 
        new=Customer(self.name)
        new.degree=self.degree
        new.color=self.color
        new.adjacentree=self.adjacentree
        return new
    '''
    #Use bidirectional breath first search method to check if there is customer in this Customer's degreeth degree friends
    def Testfraud(self, customername, maxdegree, customerdict): 
        Q=Queue.Queue()  #Store nodes that need to be processed and connect to self customer
        T=Queue.Queue()  #Store nodes that need to be processed and connect to target customer
        F=Queue.Queue()  #Store node that has been processed
        my_int_degree=int(maxdegree/2)+(maxdegree%2)
        customer_int_degree=maxdegree-my_int_degree
        Q.put(customerdict[self.name])
        T.put(customerdict[customername])
        self.color = 'R'
        F.put(self)
        self.degree = 1
        customerdict[customername].color='B'
        F.put(customerdict[customername])
        customerdict[customername].degree=1
        while not Q.empty() or not T.empty():
            if not Q.empty():
                process_node=Q.get()
                if process_node.degree>my_int_degree:
                    Whiten(F)
                    return True
                else:
                    nodelist=process_node.adjacentree.getlist()
                    for i in nodelist:
                        if customerdict[i].color=='B':
                            Whiten(F)
                            return False
                        if customerdict[i].color=='W':
                            customerdict[i].color='R'
                            F.put(customerdict[i])
                            if process_node.degree<my_int_degree: 
                                Q.put(customerdict[i])
                                customerdict[i].degree=process_node.degree+1
            if not T.empty():
                process_node=T.get()
                F.put(process_node)
                if process_node.degree>customer_int_degree:
                    Whiten(F)
                    return True
                else:
                    nodelist=process_node.adjacentree.getlist()
                    for i in nodelist:
                        if customerdict[i].color=='R':
                            Whiten(F)
                            return False
                        if customerdict[i].color=='W':
                            customerdict[i].color='B'
                            F.put(customerdict[i])
                            if process_node.degree<customer_int_degree: 
                                T.put(customerdict[i])
                                customerdict[i].degree=process_node.degree+1
        Whiten(F)
        return True


# In[49]:

# A dictionary is used to store adjacent customer. Add one customer and find one customer will take time O(1)
class Adjacentree: 
    def __init__(self):
        self.root=None
        self.adjacentnodes={}
    #directly add node object to Adjacentree
    def add_node(self, node):
        self.adjacentnodes[node.name]=1
        
    def find(self, nodename): 
        return (nodename in self.adjacentnodes)

    #get a list with all customers' names
    def getlist(self):
        return self.adjacentnodes.keys()


# In[93]:

#Change all nodes colors to W(white) in queue Q
def Whiten(Q):
    while not Q.empty():
            erase=Q.get()
            erase.color='W'
            erase.degree=0


# In[ ]:

if __name__ == '__main__':
    
    #Get all the input and output files
    try:
        batch_input_file = sys.argv[1]
        stream_input_file = sys.argv[2]
        output_file1 = sys.argv[3]
	output_file2 = sys.argv[4]
	output_file3 = sys.argv[5]
    except:
        print('Provide Correct Filename!')
        batch_input_file = raw_input('batch_input file : \n')
        stream_input_file = raw_input('stream_input_file: \n')
        output_file1 = raw_input('output_file1: \n')
	output_file2 = raw_input('output_file2: \n')
	output_file3 = raw_input('output_file3: \n')	
    
    output1=open(output_file1, 'w')
    output2=open(output_file2, 'w')
    output3=open(output_file3, 'w')
    with open(batch_input_file,'r') as batch_input:
        #Customers will be stored in Dictionary because access one element and x in dict takes average time O(1)
        CustomerDict={}
        #skip the first line with header
        next(batch_input)
        #Built CustomerDict with transactions information from batch_input
        for line in batch_input:
            data=[x.strip() for x in line.strip().split(',', 4)]
            if len(data)==5:
                time, id1, id2, amount, message=data
                if not (id1 in CustomerDict):
                    CustomerDict[id1]=Customer(id1)
                if not (id2 in CustomerDict):
                    CustomerDict[id2]=Customer(id2)
                CustomerDict[id1].add_adjacentree_node(CustomerDict[id2])
                CustomerDict[id2].add_adjacentree_node(CustomerDict[id1])
            else:
                continue
        batch_input.close()
    with open(stream_input_file,'r') as  stream_input:
        #skip the first line with header
        next(stream_input)
        #Read in information from stream_input, write out fraud alert to output and store new information to CustomerDict
        for line in stream_input:
            stream_data=[x.strip() for x in line.strip().split(',', 4)]
            if len(stream_data)==5:
                time, id1, id2, amount, message=stream_data
                one = (id1 in CustomerDict)
                two = (id2 in CustomerDict)
                if not one or not two:
                    output1.write('unverified\n')
		    output2.write('unverified\n')
		    output3.write('unverified\n')    
                else:
		    if not CustomerDict[id1].Testfraud(id2, 1, CustomerDict):
                        output1.write('trusted\n')
			output2.write('trusted\n')
			output3.write('trusted\n')
		    elif not CustomerDict[id1].Testfraud(id2, 2, CustomerDict):
                        output1.write('unverified\n')
			output2.write('trusted\n')
			output3.write('trusted\n')
                    elif not CustomerDict[id1].Testfraud(id2, 4, CustomerDict):
			output1.write('unverified\n')
			output2.write('unverified\n')                        
			output3.write('trusted\n')    
                    else:
			output1.write('unverified\n')			
			output2.write('unverified\n')
                        output3.write('unverified\n')    
                if not one:
                    CustomerDict[id1]=Customer(id1)
                if not two:
                    CustomerDict[id2]=Customer(id2)
                CustomerDict[id1].add_adjacentree_node(CustomerDict[id2])
                CustomerDict[id2].add_adjacentree_node(CustomerDict[id1])
            else:
		output1.write('Input_error\n')		
		output2.write('Input_error\n')
                output3.write('Input_error\n')    
                continue
        stream_input.close()     
    
    
    output1.close()
    output2.close()
    output3.close()

