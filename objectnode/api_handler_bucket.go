// Copyright 2018 The ChubaoFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/oss"
	"github.com/gorilla/mux"
)

type CreateBucketConfiguration struct {
	xmlns              string `xml:"xmlns"`
	locationConstraint string `xml:"locationConstraint"`
}

// Head bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (o *ObjectNode) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	// do nothing
}

// Create bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (o *ObjectNode) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		mc  *master.MasterClient
		err error
	)

	log.LogInfof("Create bucket...")
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		_ = InvalidBucketName.ServeResponse(w, r)
		return
	}
	if vol, _ := o.getVol(bucket); vol != nil {
		log.LogInfof("create bucket failed: duplicated bucket name[%v]", bucket)
		_ = DuplicatedBucket.ServeResponse(w, r)
	}
	auth := parseRequestAuthInfo(r)
	var akPolicy *oss.AKPolicy
	if akPolicy, err = o.getAkInfo(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	//todo required error code？
	if mc, err = o.vm.GetMasterClient(); err != nil {
		log.LogErrorf("get master client error: err(%v)", err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	//todo what params to createVol？
	if err = mc.AdminAPI().CreateDefaultVolume(bucket, akPolicy.UserID); err != nil {
		log.LogErrorf("create bucket[%v] failed: accessKey(%v), err(%v)", bucket, auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	//todo parse body
	policy := &oss.UserPolicy{
		OwnVol: []string{bucket},
	}
	if _, err = o.mc.OSSAPI().AddPolicy(auth.accessKey, policy); err != nil {
		log.LogErrorf("add bucket[%v] policy for user[%v] error: err(%v)", bucket, auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	w.Header().Set("Location", o.region)
	return
}

// Delete bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
func (o *ObjectNode) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Delete bucket...")

	var (
		volState *proto.VolStatInfo
		mc       *master.MasterClient
		authKey  string
		err      error
	)
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		_ = InvalidBucketName.ServeResponse(w, r)
		return
	}
	auth := parseRequestAuthInfo(r)
	var akPolicy *oss.AKPolicy
	if akPolicy, err = o.getAkInfo(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	// get volume use state
	if mc, err = o.vm.GetMasterClient(); err != nil {
		log.LogErrorf("get master client error: err(%v)", err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	if volState, err = mc.ClientAPI().GetVolumeStat(bucket); err != nil {
		log.LogErrorf("get bucket state from master error: err(%v)", err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	if volState.UsedSize != 0 {
		_ = BucketNotEmpty.ServeResponse(w, r)
		return
	}
	if err = mc.OSSAPI().DeleteVolPolicy(bucket); err != nil {
		log.LogErrorf("delete bucket[%v] error: delete related policy err(%v)", bucket, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	// delete volume from master
	if authKey, err = calculateAuthKey(akPolicy.UserID); err != nil {
		log.LogErrorf("delete bucket[%v] error: calculate authKey(%v) err(%v)", bucket, akPolicy.UserID, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	if err = mc.AdminAPI().DeleteVolume(bucket, authKey); err != nil {
		log.LogErrorf("delete bucket[%v] error: accessKey(%v), err(%v)", bucket, auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// release volume from volume manager
	o.vm.Release(bucket)
	return
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("List buckets...")

	var err error
	auth := parseRequestAuthInfo(r)
	var akPolicy *oss.AKPolicy
	if akPolicy, err = o.getAkInfo(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	ownVols := akPolicy.Policy.OwnVol
	var buckets = make([]*Bucket, 0)
	for _, ownVol := range ownVols {
		var bucket = &Bucket{Name: ownVol, CreationDate: time.Now()} //todo time
		buckets = append(buckets, bucket)
	}

	owner := &Owner{DisplayName: akPolicy.AccessKey, Id: akPolicy.AccessKey}
	listBucketOutput := &ListBucketsOutput{
		Buckets: &Buckets{Bucket: buckets},
		Owner:   owner,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(listBucketOutput); marshalError != nil {
		log.LogErrorf("listBucketsHandler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("listBucketsHandler: write response body fail, requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocation(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("getBucketLocation: get bucket location: requestID(%v)", GetRequestID(r))
	var output = &GetBucketLocationOutput{
		LocationConstraint: o.region,
	}
	var marshaled []byte
	var err error
	if marshaled, err = MarshalXMLEntity(output); err != nil {
		log.LogErrorf("getBucketLocation: marshal result fail: requestID(%v) err(%v)", GetRequestID(r), err)
		ServeInternalStaticErrorResponse(w, r)
		return
	}
	if _, err = w.Write(marshaled); err != nil {
		log.LogErrorf("getBucketLocation: write response body fail: requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

// Get bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (o *ObjectNode) getBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var param *RequestParam
	if param, err = o.parseRequestParam(r); err != nil {
		log.LogErrorf("getBucketTaggingHandler: parse request param fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}
	if param.vol == nil {
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = param.vol.GetXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("getBucketTaggingHandler: volume get XAttr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)
	var output = NewGetBucketTaggingOutput()
	if err = json.Unmarshal(ossTaggingData, output); err != nil {
		log.LogErrorf("getBucketTaggingHandler: decode tagging from json fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	var encoded []byte
	if encoded, err = MarshalXMLEntity(output); err != nil {
		log.LogErrorf("getBucketTaggingHandler: encode output fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	if _, err = w.Write(encoded); err != nil {
		log.LogErrorf("getBucketTaggingHandler: write response fail: requestID(%v) err（%v)", GetRequestID(r), err)
	}
	return
}

// Put bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (o *ObjectNode) putBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var param *RequestParam
	if param, err = o.parseRequestParam(r); err != nil {
		log.LogErrorf("putBucketTaggingHandler: parse request param fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}
	if param.vol == nil {
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
		log.LogErrorf("putBucketTaggingHandler: read request body data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var tagging = NewTagging()
	if err = UnmarshalXMLEntity(requestBody, tagging); err != nil {
		log.LogWarnf("putBucketTaggingHandler: decode request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var encoded []byte
	if encoded, err = json.Marshal(tagging); err != nil {
		log.LogWarnf("putBucketTaggingHandler: encode tagging data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	if err = param.vol.SetXAttr("/", XAttrKeyOSSTagging, encoded); err != nil {
		_ = InternalError.ServeResponse(w, r)
		return
	}

	return
}

// Delete bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (o *ObjectNode) deleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		param *RequestParam
		err   error
	)
	if param, err = o.parseRequestParam(r); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: parse request param fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var volume Volume
	if len(param.bucket) == 0 {
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}
	if volume, err = o.vm.Volume(param.bucket); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)", GetRequestID(r), param.bucket, err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}
	if err = volume.DeleteXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: volume delete tagging xattr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	return
}

func calculateAuthKey(key string) (authKey string, err error) {
	h := md5.New()
	_, err = h.Write([]byte(key))
	if err != nil {
		log.LogErrorf("calculateAuthKey: calculate auth key fail: key[%v] err[%v]", key, err)
		return
	}
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr)), nil
}

func (o *ObjectNode) getAkInfo(accessKey string) (*oss.AKPolicy, error) {
	var err error
	akPolicy, exit := o.userStore.Get(accessKey)
	if !exit {
		if akPolicy, err = o.mc.OSSAPI().GetAKInfo(accessKey); err != nil {
			log.LogInfof("load user policy err: %v", err)
			return akPolicy, err
		}
		o.userStore.Put(accessKey, akPolicy)
	}
	return akPolicy, err
}
