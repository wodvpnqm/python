# http://www.linuxidc.com/Linux/2012-06/63652p2.htm
from PyQt4.QtGui import *  
from PyQt4.QtCore import *  
import sys  
  
app = QApplication(sys.argv)  
b = QPushButton("Hello Kitty!")  
b.show()  
app.connect(b, SIGNAL("clicked()"), app, SLOT("quit()"))  
app.exec_()  
