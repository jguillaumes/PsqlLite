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

    XCTAssertThrowsSpecificNamed([pst setIntParmWithIndex:1 value: 999], NSException,
                                 @"paramOutOfRange");

    pres = [pst executeQuery:&error];
    
    NSMutableData *data = [pres getBytesWithIndex:0];
    [data writeToFile:@"/temp/thepic.jpg" atomically:true];
    
    XCTAssertThrowsSpecificNamed([pres getStringWithName:@"notpresent"], NSException,
                                 @"columnNotFound");
    XCTAssertThrowsSpecificNamed([pres getStringWithIndex:2], NSException,
                                 @"colOutOfRange");

    [pst close];
    
    [pres close];
    [pconn close];
    XCTAssert(!pconn.isConnected);
    
}

-(void) testRepeat {
    PsqlConnection *pconn = [PsqlConnection alloc];
    XCTAssertNotNil(pconn);
    NSError *error;
    
    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftw" password:@"ftw" error:&error];
    XCTAssert(pconn.isConnected);
    
    
    PsqlStatement *pst = [[PsqlStatement alloc] initWithString:@"select idviatge, nomviatge, dataviatge from viatge where idusuari = $1"
                                                  pqConnection:pconn];
    [pst prepare:&error];
    [pst setStringParmWithIndex:0 value:@"jguillaumes"];
    
    PsqlResult *pres = [pst executeQuery:&error];
    int count1 = [pres rowCount];
    [pres close];
    
    // [pst close];
    
    pres = [pst executeQuery:&error];
    if (error != NULL) {
        NSLog(@"Error: %@", error);
    }
    
    int count2 = [pres rowCount];
    XCTAssertEqual(count1, count2);
    NSLog(@"Count1: %d, Count2: %d",count1,count2);
    
    [pres close];
    [pst close];
    [pconn close];
}

-(void) testDates {
    PsqlConnection *pconn = [PsqlConnection alloc];
    XCTAssertNotNil(pconn);
    NSError *error;
    
    [pconn connectWithUrl:@"postgres://localhost:5432/ftw" userName:@"ftw" password:@"ftw" error:&error];
    XCTAssert(pconn.isConnected);
    
    
    PsqlStatement *pst = [[PsqlStatement alloc] initWithString:@"select idfoto, nomfoto, datafoto from foto where idviatge = $1"
                                                  pqConnection:pconn];
    [pst prepare:&error];
    [pst setLongParmWithIndex:0 value:2];

    PsqlResult *pres = [pst executeQuery: &error];
    XCTAssertNil(error);
    if (error != NULL) {
        NSLog(@"Error: %@", error);
    }
    
    while(![pres isEOF]) {
        int idfoto =    [pres getIntWithName:@"idfoto"];
        NSDate *data =  [pres getDateTimeWithName:@"datafoto"];
        NSDate *datan = [pres getDateTimeWithIndex:2];
        NSString *nom = [pres getStringWithName:@"nomfoto"];
        NSLog(@"Id: %d, data=%@, datan=%@, nom=[%@]", idfoto, data, datan, nom);
        [pres nextRow];
    }
    [pres close];
    [pst close];
    [pconn close];
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
