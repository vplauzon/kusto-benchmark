# Coding standard

All lines in markdown or C# files must be fewer than 100 characters.

Files must end with exactly one newline character and no blank line after the final
content. The last visible line in a C# file must be the closing brace of the final
block. If a file editor shows a blank final line, remove it before checking in the
file.

Each class, record & interface should have its own file.

#	Interfaces

Ideally, interfaces should be implemented explicitly.

#	Inner classes

Inner classes should be declared at the top of a class code, within a region named "inner types".