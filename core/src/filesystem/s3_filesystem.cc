/**
 * @file   hdfs_filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file includes implementations of S3 filesystem functions.
 */

#ifdef HAVE_S3
//#include <aws/core/client/ClientConfiguration.h>
//#include <aws/s3/S3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
//#include <aws/core/auth/AWSCredentialsProviderChain.h>

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/platform/Platform.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils;
using namespace Aws;

static const char* DIR_SUFFIX = ".dir";
static const int TIMEOUT_MAX = 10;

static std::shared_ptr<S3Client> client = nullptr;
static SDKOptions options;
static std::unordered_map<std::string, String> multipartUploadIDs;
static std::unordered_map<std::string, int> multipartUploadPartNumber;
static std::unordered_map<std::string, CompleteMultipartUploadRequest>
    multipartCompleteMultipartUploadRequest;
static std::unordered_map<std::string, CompletedMultipartUpload>
    multipartCompleteMultipartUpload;
#endif

#include <fstream>
#include <iostream>

#include "constants.h"
#include "logger.h"
#include "utils.h"

namespace tiledb {

namespace s3 {

#ifdef HAVE_S3

Status connect() {
  InitAPI(options);
  ClientConfiguration config;
  config.region = Aws::Region::US_EAST_1;
  config.scheme = Scheme::HTTP;
  //            config.connectTimeoutMs = 30000;
  //            config.requestTimeoutMs = 30000;
  //            config.readRateLimiter = Limiter;
  //            config.writeRateLimiter = Limiter;
  //            config.executor =
  //            Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG,
  //            4);
  config.endpointOverride = "localhost:9000";

  client = Aws::MakeShared<S3Client>(
      "tag111",
      Aws::MakeShared<DefaultAWSCredentialsProviderChain>("tag111"),
      config,
      false /*signPayloads*/,
      false /*useVirtualAddressing*/);
  return Status::Ok();
}

Status disconnect() {
  for (const auto& record : multipartCompleteMultipartUploadRequest) {
    std::cout << "Flushing: " << record.first << std::endl;
    CompletedMultipartUpload completedMultipartUpload =
        multipartCompleteMultipartUpload[record.first];

    std::cout << "Size: " << completedMultipartUpload.GetParts().size()
              << std::endl;

    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        record.second;
    completeMultipartUploadRequest.WithMultipartUpload(
        completedMultipartUpload);
    CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
        client->CompleteMultipartUpload(completeMultipartUploadRequest);
    std::cout << "Success: " << completeMultipartUploadOutcome.IsSuccess()
              << std::endl;
    std::cout << "Error: "
              << completeMultipartUploadOutcome.GetError().GetMessage()
              << std::endl;
  }
  Aws::ShutdownAPI(options);
  return Status::Ok();
}

Status flush_file(const URI& uri){
  Aws::Http::URI aws_uri = uri.c_str();
  String path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
    CompletedMultipartUpload completedMultipartUpload =
        multipartCompleteMultipartUpload[path_c_str];

    std::cout << "Size: " << completedMultipartUpload.GetParts().size()
              << std::endl;

    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        multipartCompleteMultipartUploadRequest[path_c_str];
    completeMultipartUploadRequest.WithMultipartUpload(
        completedMultipartUpload);
    CompleteMultipartUploadOutcome completeMultipartUploadOutcome =
        client->CompleteMultipartUpload(completeMultipartUploadRequest);
    std::cout << "Success: " << completeMultipartUploadOutcome.IsSuccess()
              << std::endl;
    std::cout << "Error: "
              << completeMultipartUploadOutcome.GetError().GetMessage()
              << std::endl;
  return Status::Ok();
//    std::this_thread::sleep_for(std::chrono::seconds(20));
//    multipartUploadIDs.erase(path_c_str);
//    multipartUploadPartNumber.erase(path_c_str);
    //multipartCompleteMultipartUploadRequest.erase(path_c_str);
//    multipartCompleteMultipartUpload.erase(path_c_str);
}

Status create_bucket(const char* bucket) {
  CreateBucketRequest createBucketRequest;
  createBucketRequest.SetBucket(bucket);
  client->CreateBucket(createBucketRequest);
  return Status::Ok();
}

void emptyBucket(const Aws::String& bucketName) {
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(bucketName);

  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return;

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(bucketName);
    deleteObjectRequest.SetKey(object.GetKey());
    client->DeleteObject(deleteObjectRequest);
  }
}

void waitForBucketToEmpty(const Aws::String& bucketName) {
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(bucketName);

  unsigned checkForObjectsCount = 0;
  while (checkForObjectsCount++ < TIMEOUT_MAX) {
    ListObjectsOutcome listObjectsOutcome =
        client->ListObjects(listObjectsRequest);

    if (listObjectsOutcome.GetResult().GetContents().size() > 0) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
}

Status delete_bucket(const char* bucket) {
  HeadBucketRequest headBucketRequest;
  headBucketRequest.SetBucket(bucket);
  HeadBucketOutcome bucketOutcome = client->HeadBucket(headBucketRequest);

  if (bucketOutcome.IsSuccess()) {
    emptyBucket(bucket);
    waitForBucketToEmpty(bucket);

    DeleteBucketRequest deleteBucketRequest;
    deleteBucketRequest.SetBucket(bucket);

    DeleteBucketOutcome deleteBucketOutcome =
        client->DeleteBucket(deleteBucketRequest);
  }  
  return Status::Ok();
}

Status create_dir(const URI& uri) {
  Aws::Http::URI aws_uri = uri.c_str();
  PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath() + DIR_SUFFIX)
      .WithBucket(aws_uri.GetAuthority());

  auto requestStream = Aws::MakeShared<Aws::StringStream>("s3-sample");
  //  *requestStream << "Folder";
  putObjectRequest.SetBody(requestStream);

  auto putObjectOutcome = client->PutObject(putObjectRequest);

  if (!putObjectOutcome.IsSuccess()) {
    std::cout << "Error while putting Object "
              << putObjectOutcome.GetError().GetExceptionName() << " "
              << putObjectOutcome.GetError().GetMessage() << std::endl;
    return LOG_STATUS(Status::IOError(
        "Error while creating directory " +
        uri.to_string()));  //+ putObjectOutcome.GetError().GetExceptionName() +
                            // putObjectOutcome.GetError().GetMessage()));
  }
  return Status::Ok();
}

bool is_dir(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath() + DIR_SUFFIX);
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return false;
  if (listObjectsOutcome.GetResult().GetContents().size() == 0)
    return false;
  if (listObjectsOutcome.GetResult().GetContents()[0].GetKey() ==
      aws_uri.GetPath() + DIR_SUFFIX)
    return true;
  return false;
}

Status move_path(const URI& old_uri, const URI& new_uri) {
  return Status::Ok();
}

bool is_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return false;
  if (listObjectsOutcome.GetResult().GetContents().size() == 0)
    return false;
  if (listObjectsOutcome.GetResult().GetContents()[0].GetKey() ==
      aws_uri.GetPath() + DIR_SUFFIX)
    return true;
  return false;
}

Status create_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.c_str();
  PutObjectRequest putObjectRequest;
  putObjectRequest.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto requestStream = Aws::MakeShared<Aws::StringStream>("s3-sample");
  //  *requestStream << "Folder";
  putObjectRequest.SetBody(requestStream);

  auto putObjectOutcome = client->PutObject(putObjectRequest);

  if (!putObjectOutcome.IsSuccess()) {
    std::cout << "Error while putting Object "
              << putObjectOutcome.GetError().GetExceptionName() << " "
              << putObjectOutcome.GetError().GetMessage() << std::endl;
    return LOG_STATUS(Status::IOError(
        "Error while creating directory " +
        uri.to_string()));  //+ putObjectOutcome.GetError().GetExceptionName() +
                            // putObjectOutcome.GetError().GetMessage()));
  }
  return Status::Ok();
}

Status remove_file(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing directory " + uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    if (listObjectsOutcome.GetResult().GetContents()[0].GetKey() ==
        aws_uri.GetPath()) {
      DeleteObjectRequest deleteObjectRequest;
      deleteObjectRequest.SetBucket(aws_uri.GetAuthority());
      deleteObjectRequest.SetKey(object.GetKey());
      client->DeleteObject(deleteObjectRequest);
    }
  }
  return Status::Ok();
}

Status remove_path(const URI& uri) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing directory " + uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    DeleteObjectRequest deleteObjectRequest;
    deleteObjectRequest.SetBucket(aws_uri.GetAuthority());
    deleteObjectRequest.SetKey(object.GetKey());
    client->DeleteObject(deleteObjectRequest);
  }
  return Status::Ok();
}

Status read_from_file(
    const URI& uri, off_t offset, void* buffer, uint64_t length) {
  Aws::Http::URI aws_uri = uri.c_str();
  GetObjectRequest getObjectRequest;
  getObjectRequest.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  getObjectRequest.SetRange(("bytes="+std::to_string(offset)+"-"+std::to_string(offset+length)).c_str());
  std::cout<<"Range: " << getObjectRequest.GetRange()<<std::endl;  
  getObjectRequest.SetResponseStreamFactory([buffer, length]() {
    std::unique_ptr<Aws::StringStream> stream(
        Aws::New<Aws::StringStream>("alloc1"));

    stream->rdbuf()->pubsetbuf(static_cast<char*>(buffer), length);

    return stream.release();
  });

  auto getObjectOutcome = client->GetObject(getObjectRequest);

  if (!getObjectOutcome.IsSuccess()) {
    std::cout << "Error while getting object "
              << getObjectOutcome.GetError().GetExceptionName() << " "
              << getObjectOutcome.GetError().GetMessage() << std::endl;
  }
  return Status::Ok();
}

Status write_to_file(
    const URI& uri, const void* buffer, const uint64_t length) {
  Aws::Http::URI aws_uri = uri.c_str();
  String path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  if (multipartUploadIDs.find(path_c_str) == multipartUploadIDs.end()) {
    // open file
    std::cout << "Openning file: " << path_c_str << std::endl;
    CreateMultipartUploadRequest createMultipartUploadRequest;
    createMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
    createMultipartUploadRequest.SetKey(path);
    createMultipartUploadRequest.SetContentType("application/octet-stream");

    CreateMultipartUploadOutcome createMultipartUploadOutcome =
        client->CreateMultipartUpload(createMultipartUploadRequest);
    multipartUploadIDs[path_c_str] =
        createMultipartUploadOutcome.GetResult().GetUploadId();
    multipartUploadPartNumber[path_c_str] = 0;
    CompleteMultipartUploadRequest completeMultipartUploadRequest;
    completeMultipartUploadRequest.SetBucket(aws_uri.GetAuthority());
    completeMultipartUploadRequest.SetKey(path);
    completeMultipartUploadRequest.SetUploadId(multipartUploadIDs[path_c_str]);

    CompletedMultipartUpload completedMultipartUpload;
    multipartCompleteMultipartUpload[path_c_str] = completedMultipartUpload;
    multipartCompleteMultipartUploadRequest[path_c_str] =
        completeMultipartUploadRequest;
  }
  multipartUploadPartNumber[path_c_str]++;
  std::shared_ptr<Aws::StringStream> stream =
      Aws::MakeShared<Aws::StringStream>("alloc");

  stream->rdbuf()->pubsetbuf(
      static_cast<char*>(const_cast<void*>(buffer)), length);
  stream->rdbuf()->pubseekpos(length);

  stream->seekg(0);
  UploadPartRequest uploadPartRequest;
  uploadPartRequest.SetBucket(aws_uri.GetAuthority());
  uploadPartRequest.SetKey(path);
  uploadPartRequest.SetPartNumber(multipartUploadPartNumber[path_c_str]);
  uploadPartRequest.SetUploadId(multipartUploadIDs[path_c_str]);
  uploadPartRequest.SetBody(stream);
  // uploadPartRequest.SetContentMD5(HashingUtils::Base64Encode(md5OfStream));

  uploadPartRequest.SetContentLength(length);

  UploadPartOutcomeCallable uploadPartOutcomeCallable =
      client->UploadPartCallable(uploadPartRequest);
  UploadPartOutcome uploadPartOutcome = uploadPartOutcomeCallable.get();
  CompletedPart completedPart;
  completedPart.SetETag(uploadPartOutcome.GetResult().GetETag());
  completedPart.SetPartNumber(multipartUploadPartNumber[path_c_str]);
  multipartCompleteMultipartUpload[path_c_str].AddParts(completedPart);

  return Status::Ok();
}

Status ls(const URI& uri, std::vector<std::string>* paths) {
  Aws::Http::URI aws_uri = (uri.to_path() + std::string("/")).c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  listObjectsRequest.WithDelimiter("/");
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing directory " + uri.to_string()));

  for (const auto& object : listObjectsOutcome.GetResult().GetContents()) {
    paths->push_back(object.GetKey().c_str());

    // std::cout << object.GetKey() << std::endl;
  }
  return Status::Ok();
}

Status file_size(const URI& uri, uint64_t* nbytes) {
  Aws::Http::URI aws_uri = uri.to_path().c_str();
  ListObjectsRequest listObjectsRequest;
  listObjectsRequest.SetBucket(aws_uri.GetAuthority());
  listObjectsRequest.SetPrefix(aws_uri.GetPath());
  ListObjectsOutcome listObjectsOutcome =
      client->ListObjects(listObjectsRequest);

  if (!listObjectsOutcome.IsSuccess())
    return LOG_STATUS(
        Status::IOError("Error while listing directory " + uri.to_string()));
  if (listObjectsOutcome.GetResult().GetContents().size() < 1)
    return LOG_STATUS(
        Status::IOError(std::string("Not a file ") + uri.to_string()));
  *nbytes = static_cast<uint64_t>(
      listObjectsOutcome.GetResult().GetContents()[0].GetSize());
  return Status::Ok();
}

#endif
}
}  // namespace tiledb
