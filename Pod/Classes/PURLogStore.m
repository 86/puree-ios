//
//  PURLogStore.m
//  Puree
//
//  Created by tomohiro-moro on 10/7/14.
//  Copyright (c) 2014 Tomohiro Moro. All rights reserved.
//

#import <YapDatabase.h>
#import "PURLogStore.h"
#import "PURLog.h"
#import "PUROutput.h"

#import "YapDatabaseView.h"

static NSString * const LogDatabaseDirectory = @"com.cookpad.PureeData.default";
static NSString * const LogDatabaseFileName = @"logs.db";

static NSString * const LogDataCollectionNamePrefix = @"log_";
static NSString * const SystemDataCollectionNamePrefix = @"system_";

static NSString * const LogMetadataKeyOutput = @"_MetadataOutput";

static NSString * const ViewExtentionDateAscending = @"date_ascending";

static NSMutableDictionary *__databases;

@interface PURLogStore ()

@property (nonatomic) NSString *databasePath;
@property (nonatomic) YapDatabase *database;
@property (nonatomic) YapDatabaseConnection *databaseConnection;

@end

NSString *PURLogStoreCollectionNameForPattern(NSString *pattern)
{
    return [LogDataCollectionNamePrefix stringByAppendingString:pattern];
}

NSDictionary *PURLogStoreMetadataForLog(PURLog *log, PUROutput *output)
{
    return @{
             LogMetadataKeyOutput: NSStringFromClass([output class]),
             };
}

NSString *PURLogKey(PUROutput *output, PURLog *log)
{
    return [[NSStringFromClass([output class]) stringByAppendingString:@"_"] stringByAppendingString:log.identifier];
}

@implementation PURLogStore

+ (void)initialize
{
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        __databases = [NSMutableDictionary new];
    });
}

+ (instancetype)defaultLogStore
{
    return [[self alloc] initWithDatabasePath:[self defaultDatabasePath]];
}

- (instancetype)initWithDatabasePath:(NSString *)databasePath
{
    self = [super init];
    if (self) {
        _databasePath = databasePath;
    }
    return self;
}

- (BOOL)prepare
{
    NSFileManager *fileManager = [NSFileManager defaultManager];
    NSString *databaseDirectory = [self.databasePath stringByDeletingLastPathComponent];
    BOOL isDirectory = NO;
    if (![fileManager fileExistsAtPath:databaseDirectory isDirectory:&isDirectory]) {
        NSError *error = nil;
        [fileManager createDirectoryAtPath:databaseDirectory
               withIntermediateDirectories:YES
                                attributes:nil
                                     error:&error];
        if (error) {
            return NO;
        }
    } else if (!isDirectory) {
        return NO;
    }

    YapDatabase *database = __databases[self.databasePath];
    if (!database) {
        database = [[YapDatabase alloc] initWithPath:self.databasePath];
        __databases[self.databasePath] = database;
    }
    self.database = database;
    
    [self registerView];
    
    self.databaseConnection = [self.database newConnection];

    return self.database && self.databaseConnection;
}

+ (NSString *)defaultDatabasePath
{
    NSArray *libraryCachePaths = NSSearchPathForDirectoriesInDomains(NSCachesDirectory, NSUserDomainMask, YES);
    NSString *libraryCacheDirectoryPath = libraryCachePaths.firstObject;
    NSString *filePath = [LogDatabaseDirectory stringByAppendingPathComponent:LogDatabaseFileName];
    NSString *databasePath = [libraryCacheDirectoryPath stringByAppendingPathComponent:filePath];

    return databasePath;
}

- (void)retrieveLogsForPattern:(NSString *)pattern output:(PUROutput *)output completion:(PURLogStoreRetrieveCompletionBlock)completion;
{
    NSAssert(self.databaseConnection, @"Database connection is not available");
    
    [self.databaseConnection readWithBlock:^(YapDatabaseReadTransaction *transaction) {
        NSMutableArray *logs = [NSMutableArray new];
        NSString *keyPrefix = [NSStringFromClass([output class]) stringByAppendingString:@"_"];
        NSString *collectionName = PURLogStoreCollectionNameForPattern(output.tagPattern);
        NSRange range = NSMakeRange(0, [transaction numberOfKeysInCollection:collectionName]);
        [[transaction ext:ViewExtentionDateAscending] enumerateRowsInGroup:collectionName
                                                               withOptions:0
                                                                     range:range
                                                                usingBlock:^(NSString *collection, NSString *key, PURLog *log, id metadata, NSUInteger index, BOOL *stop){
                                                                    [logs addObject:log];
                                                                } withFilter:^BOOL(NSString *collection, NSString *key) {
                                                                    return [key hasPrefix:keyPrefix];
                                                                }];
        completion(logs);
    }];
}

- (void)addLog:(PURLog *)log fromOutput:(PUROutput *)output
{
    NSAssert(self.databaseConnection, @"Database connection is not available");

    if (![log isKindOfClass:[PURLog class]]) {
        return;
    }

    [self.databaseConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction){
        NSString *collectionName = PURLogStoreCollectionNameForPattern(output.tagPattern);
        [transaction setObject:log forKey:PURLogKey(output, log) inCollection:collectionName];
    }];
}

- (void)addLogs:(NSArray *)logs fromOutput:(PUROutput *)output
{
    NSAssert(self.databaseConnection, @"Database connection is not available");

    [self.databaseConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction){
        NSString *collectionName = PURLogStoreCollectionNameForPattern(output.tagPattern);
        for (PURLog *log in logs) {
            if (![log isKindOfClass:[PURLog class]]) {
                continue;
            }
            [transaction setObject:log forKey:PURLogKey(output, log) inCollection:collectionName];
        }
    }];
}

- (void)removeLogs:(NSArray *)logs fromOutput:(PUROutput *)output
{
    NSAssert(self.databaseConnection, @"Database connection is not available");

    [self.databaseConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction){
        NSString *collectionName = PURLogStoreCollectionNameForPattern(output.tagPattern);
        for (PURLog *log in logs) {
            if (![log isKindOfClass:[PURLog class]]) {
                continue;
            }
            [transaction removeObjectForKey:PURLogKey(output, log) inCollection:collectionName];
        }
    }];
}

- (void)clearAll
{
    NSAssert(self.databaseConnection, @"Database connection is not available");

    [self.databaseConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction){
        [transaction removeAllObjectsInAllCollections];
    }];
}

- (void)reduceStoredLogsWithLimit:(NSInteger)limit fromOutput:(PUROutput *)output
{
    NSAssert(self.databaseConnection, @"Database connection is not available");
    
    [self.databaseConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
        NSString *collectionName = PURLogStoreCollectionNameForPattern(output.tagPattern);
        NSInteger over = [transaction numberOfKeysInCollection:collectionName] - limit;
        if (over <= 0) { return; }
        
        NSMutableArray *removeKeys = [NSMutableArray new];
        [[transaction ext:ViewExtentionDateAscending] enumerateKeysAndObjectsInGroup:collectionName usingBlock:^(NSString *collection, NSString *key, PURLog *log, NSUInteger index, BOOL *stop) {
            [removeKeys addObject:key];
            if (index == over - 1) { *stop = YES; }
        }];
        
        if (removeKeys.count > 0) {
            [transaction removeObjectsForKeys:removeKeys inCollection:PURLogStoreCollectionNameForPattern(output.tagPattern)];
        }
    }];
}

- (void)registerView
{
    YapDatabaseViewGrouping *grouping = [YapDatabaseViewGrouping withObjectBlock:^NSString *(NSString *collection, NSString *key, id object) {
        return collection;
    }];
    YapDatabaseViewSorting *sortingByDate = [YapDatabaseViewSorting withObjectBlock:^NSComparisonResult(NSString *group, NSString *collection1, NSString *key1, PURLog *object1, NSString *collection2, NSString *key2, PURLog *object2) {
        return [object1.date compare:object2.date];
    }];
    YapDatabaseView *dateAscendingView = [[YapDatabaseView alloc] initWithGrouping:grouping sorting:sortingByDate];
    [self.database registerExtension:dateAscendingView withName:ViewExtentionDateAscending];
}

@end
