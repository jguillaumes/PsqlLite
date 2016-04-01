//
//  PsqlLiteTests.m
//  PsqlLiteTests
//
//  Created by Jordi Guillaumes Pons on 27/3/16.
//  Copyright © 2016 Jordi Guillaumes Pons. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "PsqlLite.h"

@interface PsqlLiteTests : XCTestCase

@end

@implementation PsqlLiteTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testConnection {
    PsqlConnection *pconn = [[PsqlConnection alloc] init];
    NSError *error;

    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftw" password:@"ftw" error:&error];
    XCTAssert( [pconn isConnected] );
    [pconn close];
    pconn = nil;
    
    pconn = [[PsqlConnection alloc] init];
    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftwu" password:@"ftw" error:&error];
    XCTAssertNotNil(error);
    NSLog(@"Error de connexió: %@\n", error);
    pconn = nil;
    
    pconn = [[PsqlConnection alloc] init];
    [pconn connectWithUrl:@"postgres://doesnotexist:5432/ftw" userName:@"doesntmatter" password:@"doesntmatter" error:&error];
    XCTAssertNotNil(error);
    NSLog(@"Error de connexió: %@\n", error);
    pconn = nil;
    
}

- (void) testPrepare {
    NSError *error;
    
    PsqlConnection *pconn = [PsqlConnection alloc];
    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftw" password:@"ftw" error:&error];
    XCTAssert(pconn.isConnected);

    PsqlStatement *pst = [[PsqlStatement alloc] initWithString:@"select idviatge, nomviatge, dataviatge from viatge where idusuari = $1"
                                                  pqConnection:pconn];
    XCTAssertNotNil(pst);
    XCTAssert(![pst isOK]);
    
    [pst prepare:&error];
    XCTAssertNil(error);
    XCTAssert([pst isOK]);
    
    pst = nil;
    pst = [[PsqlStatement alloc] initWithString:@"garbage sql statement"
                                   pqConnection:pconn];
    [pst prepare:&error];
    XCTAssertNotNil(error);
    NSLog(@"Expected prepare error: %@", error);
    
    [pst close];
}

- (void)testExecute {
    PsqlConnection *pconn = [PsqlConnection alloc];
    XCTAssertNotNil(pconn);
    NSError *error;
    
    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftw" password:@"ftw" error:&error];
    XCTAssert(pconn.isConnected);

    
    PsqlStatement *pst = [[PsqlStatement alloc] initWithString:@"select idviatge, nomviatge, dataviatge from viatge where idusuari = $1"
                                                  pqConnection:pconn];
    [pst prepare:&error];
    XCTAssert(pst.isOK);

    [pst setStringParmWithIndex:0 value:@"jguillaumes"];
    
    PsqlResult *pres = [pst executeQuery:&error];
    XCTAssertNotNil(pres);
    XCTAssert(![pres isEmpty]);
    while(![pres isEOF]) {
        NSNumber *idviatge   = [pres getNumberWithName:@"idviatge"];
        NSString *nomViatge  = [pres getStringWithName:@"nomviatge"];
        NSDate   *dataviatge = [pres getDateWithIndex:2];
        NSLog([NSString stringWithFormat:@"%@ - %@: %@", idviatge, nomViatge, dataviatge]);
        [pres nextRow];
    }
 
    [pst close];
    [pres close];
    
    [pst initWithString:@"select jpeg from jpeg where idfoto = $1" pqConnection:pconn];
    [pst prepare:&error];
    if (!pst.isOK) NSLog(@"Error de connexió: %@\n", error);
    [pst setIntParmWithIndex:0 value:1000];
    pres = [pst executeQuery:&error];
    
    NSMutableData *data = [pres getBytesWithIndex:0];
    [data writeToFile:@"/temp/thepic.jpg" atomically:true];
    
    [pst close];
    
    [pres close];
    [pconn close];
    XCTAssert(!pconn.isConnected);
    
}
/*
- (void)testPerformanceExample {
    // This is an example of a performance test case.
    [self measureBlock:^{
        // Put the code you want to measure the time of here.
    }];
}
*/
@end
