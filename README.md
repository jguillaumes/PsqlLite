# Postgresql access for Cocoa applications

This project implements a small OO wrapper over the **libpq** library to allow Cocoa applications to access Postgresql databases, 
either from Objective-C or swift code. This wrapper is implemented as a OSX Framework, and it intends to be self-contained. Technically, 
it is statically linked to the libpq.a library, so it does not need to have access to the Postgresql dynamic libraries at runtime. The
public interface does not use any Postgresql definition, so it should not need the C headers in development time either.

## License

BSD-style license. Check the LICENSE.txt file in the same directory where this file resides.

## Compile

Check-out this repository from XCode, and build the corresponding project. It contains some unit test cases. Please change the tests so
they use a server, database and table available to you.

## Install

Drop the framework in /Library/Frameworks or ~/Library/Frameworks. Alternatively, drop it into the client project.

## Documentation

To be written.



