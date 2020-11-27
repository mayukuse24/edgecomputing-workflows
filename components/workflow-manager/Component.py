import random
import docker


"""
input/output types defined:
-1 - none
0 - post {data:""}
1 - post {file:...}
2 - post {text:""}
3 - post {text:"",audio:...}    don't convert 
4 - post {type:"",data:""}
5 - post {data_bytes:""}
6 - post {tone info:"",inacuracy:int()}
7 - post {text:[""]}


23 - text semantics output
24 - classify output
"""




class Component:
	internal_port = 5000
	target_port = []
	mounts =[]
	name = ""
	image = ""
	path =""
	input_type = 0
	output_type = 0
	ids = []
	status = [] #  1 created temporary aka dont share 2 created persistent -1 killed
	useable = []
	count = 0
	spec = {}
	
	def __init__(self,port,name,img,path):
		self.image = img
		self.internal_port = port
		self.name = name
		self.status += [0]
		self.target_port = []
		self.path=path
		self.spec = {}


	def get_useable(self):
		return self.useable

	def create(self,used_ports,temp):
		self.ids.append(self.count+1)
		self.status += [0]
		self.count += 1
		flag = False

		if temp == False:
			if len(self.useable) > 0:
				self.target_port += self.target_port[int(self.useable[0]-1)]
				flag =True
			else:
				self.useable += [id]
		if not flag:
			random_port = random.randint(10000, 65500)
			while random_port in used_ports:
				random_port = random.randint(10000, 65500)
			used_ports += [random_port]
			self.target_port += [random_port]
			#print(self.target_port,self.ids)
		return self.count, self.target_port[self.count-1]

	def add_spec(self,id,ser_obj):
		service_spec = {
			'name': self.name,
			'port': self.target_port[id-1],
			'service_obj': ser_obj
		}
		self.spec[id] = service_spec
