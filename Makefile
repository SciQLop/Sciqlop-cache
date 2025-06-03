##
## EPITECH PROJECT, 2024
## Makefile
## File description:
## Makefile
##

MAIN =	src/main.cpp

TEST = tests/main.cpp

SRC =

OBJ_MAIN = $(MAIN:.cpp=.o)

OBJ_TEST = $(TEST:.cpp=.o)

OBJ = $(SRC:.cpp=.o)

G	=	g++
RM	=	rm -rf

NAME	=	sciqlop_cache

CPPFLAGS = -std=c++20 -Wall -Wextra -I./include/ -lsqlite3 -lstdc++fs

all:	$(NAME)

$(NAME):	$(OBJ) $(OBJ_MAIN)
	$(G) -o $(NAME) $(OBJ)

test: 		$(OBJ) $(OBJ_TEST)
	$(G) -o $(NAME)_test $(OBJ) $(OBJ_TEST) --coverage

clean:
	$(RM) $(OBJ_MAIN)
	$(RM) $(OBJ_TEST)
	$(RM) $(OBJ)

fclean: clean
	$(RM) $(NAME)
	$(RM) $(NAME)_test

re: fclean all

.PHONY: all test clean fclean re
