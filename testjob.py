
class MyTest:
    
    def __init__(self):
        self.tasks = list()
        print "Initializing myTest"
        self.create_tasks()

    def create_tasks(self):
        print "Creating Tasks"
        self.tasks = [1,3]
        self.tasks.append(2)

    @staticmethod
    def test():
        print "hello"

    
        

#test = 'test'
#print "blah lbah %s" % \
#    ('test')

#Test.test()

t = MyTest()
#t.create_tasks()
print t.tasks
print isinstance(t, MyTest)
z = {'lol':'haha'}
print z['lol']
