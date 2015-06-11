import turtle

def draw_square():
    window = turtle.Screen()
    window.bgcolor("green")
    shane = turtle.Turtle()
    shane.shape("turtle")

    for number in range(0,4):
        shane.forward(100)
        shane.right(90)

    draw_circle()
    window.exitonclick()

def draw_circle():
    window = turtle.Screen()
    alba = turtle.Turtle()
    alba.shape("turtle")
    alba.circle(100)
    draw_triangle()
    window.exitonclick()

def draw_triangle():
    window = turtle.Screen()
    mali = turtle.Turtle()
    mali.shape("turtle")
    
    for number in range(0,3):
        mali.forward(100)
        mali.right(45)
        
    window.exitonclick()    
    

draw_square()

    
