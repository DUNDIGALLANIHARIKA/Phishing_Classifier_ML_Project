from datetime import datetime

class App_logger:
    
    def __call__(self): 
        pass

    def log(self,file_obj,log_msg):
        self.now=datetime.now()
        self.date=self.now.date()
        self.current_time=self.now.strftime("%H:%M:%S")
        file_obj.write(str(self.date)+"\t\t"+str(self.current_time)+"\t\t"+log_msg+"\n")



        