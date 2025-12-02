// coverage:ignore-file
// GENERATED CODE - DO NOT MODIFY BY HAND
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'api.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

T _$identity<T>(T value) => value;

final _privateConstructorUsedError = UnsupportedError(
    'It seems like you constructed your class using `MyClass._()`. This constructor is only meant to be used by freezed and you are not supposed to need it nor use it.\nPlease check the documentation here for more information: https://github.com/rrousselGit/freezed#adding-getters-and-methods-to-our-models');

/// @nodoc
mixin _$DirectoryListing {
  List<FileEntry> get files => throw _privateConstructorUsedError;
  List<String> get directories => throw _privateConstructorUsedError;

  /// Create a copy of DirectoryListing
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $DirectoryListingCopyWith<DirectoryListing> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $DirectoryListingCopyWith<$Res> {
  factory $DirectoryListingCopyWith(
          DirectoryListing value, $Res Function(DirectoryListing) then) =
      _$DirectoryListingCopyWithImpl<$Res, DirectoryListing>;
  @useResult
  $Res call({List<FileEntry> files, List<String> directories});
}

/// @nodoc
class _$DirectoryListingCopyWithImpl<$Res, $Val extends DirectoryListing>
    implements $DirectoryListingCopyWith<$Res> {
  _$DirectoryListingCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of DirectoryListing
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? files = null,
    Object? directories = null,
  }) {
    return _then(_value.copyWith(
      files: null == files
          ? _value.files
          : files // ignore: cast_nullable_to_non_nullable
              as List<FileEntry>,
      directories: null == directories
          ? _value.directories
          : directories // ignore: cast_nullable_to_non_nullable
              as List<String>,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$DirectoryListingImplCopyWith<$Res>
    implements $DirectoryListingCopyWith<$Res> {
  factory _$$DirectoryListingImplCopyWith(_$DirectoryListingImpl value,
          $Res Function(_$DirectoryListingImpl) then) =
      __$$DirectoryListingImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({List<FileEntry> files, List<String> directories});
}

/// @nodoc
class __$$DirectoryListingImplCopyWithImpl<$Res>
    extends _$DirectoryListingCopyWithImpl<$Res, _$DirectoryListingImpl>
    implements _$$DirectoryListingImplCopyWith<$Res> {
  __$$DirectoryListingImplCopyWithImpl(_$DirectoryListingImpl _value,
      $Res Function(_$DirectoryListingImpl) _then)
      : super(_value, _then);

  /// Create a copy of DirectoryListing
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? files = null,
    Object? directories = null,
  }) {
    return _then(_$DirectoryListingImpl(
      files: null == files
          ? _value._files
          : files // ignore: cast_nullable_to_non_nullable
              as List<FileEntry>,
      directories: null == directories
          ? _value._directories
          : directories // ignore: cast_nullable_to_non_nullable
              as List<String>,
    ));
  }
}

/// @nodoc

class _$DirectoryListingImpl implements _DirectoryListing {
  const _$DirectoryListingImpl(
      {required final List<FileEntry> files,
      required final List<String> directories})
      : _files = files,
        _directories = directories;

  final List<FileEntry> _files;
  @override
  List<FileEntry> get files {
    if (_files is EqualUnmodifiableListView) return _files;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_files);
  }

  final List<String> _directories;
  @override
  List<String> get directories {
    if (_directories is EqualUnmodifiableListView) return _directories;
    // ignore: implicit_dynamic_type
    return EqualUnmodifiableListView(_directories);
  }

  @override
  String toString() {
    return 'DirectoryListing(files: $files, directories: $directories)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$DirectoryListingImpl &&
            const DeepCollectionEquality().equals(other._files, _files) &&
            const DeepCollectionEquality()
                .equals(other._directories, _directories));
  }

  @override
  int get hashCode => Object.hash(
      runtimeType,
      const DeepCollectionEquality().hash(_files),
      const DeepCollectionEquality().hash(_directories));

  /// Create a copy of DirectoryListing
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$DirectoryListingImplCopyWith<_$DirectoryListingImpl> get copyWith =>
      __$$DirectoryListingImplCopyWithImpl<_$DirectoryListingImpl>(
          this, _$identity);
}

abstract class _DirectoryListing implements DirectoryListing {
  const factory _DirectoryListing(
      {required final List<FileEntry> files,
      required final List<String> directories}) = _$DirectoryListingImpl;

  @override
  List<FileEntry> get files;
  @override
  List<String> get directories;

  /// Create a copy of DirectoryListing
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$DirectoryListingImplCopyWith<_$DirectoryListingImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
mixin _$FileEntry {
  String get name => throw _privateConstructorUsedError;
  String get fileRefJson => throw _privateConstructorUsedError;
  BigInt get size => throw _privateConstructorUsedError;
  String? get mediaType => throw _privateConstructorUsedError;
  int? get timestamp => throw _privateConstructorUsedError;

  /// Create a copy of FileEntry
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $FileEntryCopyWith<FileEntry> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $FileEntryCopyWith<$Res> {
  factory $FileEntryCopyWith(FileEntry value, $Res Function(FileEntry) then) =
      _$FileEntryCopyWithImpl<$Res, FileEntry>;
  @useResult
  $Res call(
      {String name,
      String fileRefJson,
      BigInt size,
      String? mediaType,
      int? timestamp});
}

/// @nodoc
class _$FileEntryCopyWithImpl<$Res, $Val extends FileEntry>
    implements $FileEntryCopyWith<$Res> {
  _$FileEntryCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of FileEntry
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? name = null,
    Object? fileRefJson = null,
    Object? size = null,
    Object? mediaType = freezed,
    Object? timestamp = freezed,
  }) {
    return _then(_value.copyWith(
      name: null == name
          ? _value.name
          : name // ignore: cast_nullable_to_non_nullable
              as String,
      fileRefJson: null == fileRefJson
          ? _value.fileRefJson
          : fileRefJson // ignore: cast_nullable_to_non_nullable
              as String,
      size: null == size
          ? _value.size
          : size // ignore: cast_nullable_to_non_nullable
              as BigInt,
      mediaType: freezed == mediaType
          ? _value.mediaType
          : mediaType // ignore: cast_nullable_to_non_nullable
              as String?,
      timestamp: freezed == timestamp
          ? _value.timestamp
          : timestamp // ignore: cast_nullable_to_non_nullable
              as int?,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$FileEntryImplCopyWith<$Res>
    implements $FileEntryCopyWith<$Res> {
  factory _$$FileEntryImplCopyWith(
          _$FileEntryImpl value, $Res Function(_$FileEntryImpl) then) =
      __$$FileEntryImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call(
      {String name,
      String fileRefJson,
      BigInt size,
      String? mediaType,
      int? timestamp});
}

/// @nodoc
class __$$FileEntryImplCopyWithImpl<$Res>
    extends _$FileEntryCopyWithImpl<$Res, _$FileEntryImpl>
    implements _$$FileEntryImplCopyWith<$Res> {
  __$$FileEntryImplCopyWithImpl(
      _$FileEntryImpl _value, $Res Function(_$FileEntryImpl) _then)
      : super(_value, _then);

  /// Create a copy of FileEntry
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? name = null,
    Object? fileRefJson = null,
    Object? size = null,
    Object? mediaType = freezed,
    Object? timestamp = freezed,
  }) {
    return _then(_$FileEntryImpl(
      name: null == name
          ? _value.name
          : name // ignore: cast_nullable_to_non_nullable
              as String,
      fileRefJson: null == fileRefJson
          ? _value.fileRefJson
          : fileRefJson // ignore: cast_nullable_to_non_nullable
              as String,
      size: null == size
          ? _value.size
          : size // ignore: cast_nullable_to_non_nullable
              as BigInt,
      mediaType: freezed == mediaType
          ? _value.mediaType
          : mediaType // ignore: cast_nullable_to_non_nullable
              as String?,
      timestamp: freezed == timestamp
          ? _value.timestamp
          : timestamp // ignore: cast_nullable_to_non_nullable
              as int?,
    ));
  }
}

/// @nodoc

class _$FileEntryImpl implements _FileEntry {
  const _$FileEntryImpl(
      {required this.name,
      required this.fileRefJson,
      required this.size,
      this.mediaType,
      this.timestamp});

  @override
  final String name;
  @override
  final String fileRefJson;
  @override
  final BigInt size;
  @override
  final String? mediaType;
  @override
  final int? timestamp;

  @override
  String toString() {
    return 'FileEntry(name: $name, fileRefJson: $fileRefJson, size: $size, mediaType: $mediaType, timestamp: $timestamp)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$FileEntryImpl &&
            (identical(other.name, name) || other.name == name) &&
            (identical(other.fileRefJson, fileRefJson) ||
                other.fileRefJson == fileRefJson) &&
            (identical(other.size, size) || other.size == size) &&
            (identical(other.mediaType, mediaType) ||
                other.mediaType == mediaType) &&
            (identical(other.timestamp, timestamp) ||
                other.timestamp == timestamp));
  }

  @override
  int get hashCode =>
      Object.hash(runtimeType, name, fileRefJson, size, mediaType, timestamp);

  /// Create a copy of FileEntry
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$FileEntryImplCopyWith<_$FileEntryImpl> get copyWith =>
      __$$FileEntryImplCopyWithImpl<_$FileEntryImpl>(this, _$identity);
}

abstract class _FileEntry implements FileEntry {
  const factory _FileEntry(
      {required final String name,
      required final String fileRefJson,
      required final BigInt size,
      final String? mediaType,
      final int? timestamp}) = _$FileEntryImpl;

  @override
  String get name;
  @override
  String get fileRefJson;
  @override
  BigInt get size;
  @override
  String? get mediaType;
  @override
  int? get timestamp;

  /// Create a copy of FileEntry
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$FileEntryImplCopyWith<_$FileEntryImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
mixin _$S5Error {
  String get field0 => throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) =>
      throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) =>
      throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) =>
      throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) =>
      throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) =>
      throw _privateConstructorUsedError;
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) =>
      throw _privateConstructorUsedError;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $S5ErrorCopyWith<S5Error> get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $S5ErrorCopyWith<$Res> {
  factory $S5ErrorCopyWith(S5Error value, $Res Function(S5Error) then) =
      _$S5ErrorCopyWithImpl<$Res, S5Error>;
  @useResult
  $Res call({String field0});
}

/// @nodoc
class _$S5ErrorCopyWithImpl<$Res, $Val extends S5Error>
    implements $S5ErrorCopyWith<$Res> {
  _$S5ErrorCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_value.copyWith(
      field0: null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$S5Error_InvalidInputImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_InvalidInputImplCopyWith(_$S5Error_InvalidInputImpl value,
          $Res Function(_$S5Error_InvalidInputImpl) then) =
      __$$S5Error_InvalidInputImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_InvalidInputImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_InvalidInputImpl>
    implements _$$S5Error_InvalidInputImplCopyWith<$Res> {
  __$$S5Error_InvalidInputImplCopyWithImpl(_$S5Error_InvalidInputImpl _value,
      $Res Function(_$S5Error_InvalidInputImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_InvalidInputImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_InvalidInputImpl extends S5Error_InvalidInput {
  const _$S5Error_InvalidInputImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.invalidInput(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_InvalidInputImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_InvalidInputImplCopyWith<_$S5Error_InvalidInputImpl>
      get copyWith =>
          __$$S5Error_InvalidInputImplCopyWithImpl<_$S5Error_InvalidInputImpl>(
              this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return invalidInput(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return invalidInput?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (invalidInput != null) {
      return invalidInput(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return invalidInput(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return invalidInput?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (invalidInput != null) {
      return invalidInput(this);
    }
    return orElse();
  }
}

abstract class S5Error_InvalidInput extends S5Error {
  const factory S5Error_InvalidInput(final String field0) =
      _$S5Error_InvalidInputImpl;
  const S5Error_InvalidInput._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_InvalidInputImplCopyWith<_$S5Error_InvalidInputImpl>
      get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$S5Error_ConnectionErrorImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_ConnectionErrorImplCopyWith(
          _$S5Error_ConnectionErrorImpl value,
          $Res Function(_$S5Error_ConnectionErrorImpl) then) =
      __$$S5Error_ConnectionErrorImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_ConnectionErrorImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_ConnectionErrorImpl>
    implements _$$S5Error_ConnectionErrorImplCopyWith<$Res> {
  __$$S5Error_ConnectionErrorImplCopyWithImpl(
      _$S5Error_ConnectionErrorImpl _value,
      $Res Function(_$S5Error_ConnectionErrorImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_ConnectionErrorImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_ConnectionErrorImpl extends S5Error_ConnectionError {
  const _$S5Error_ConnectionErrorImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.connectionError(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_ConnectionErrorImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_ConnectionErrorImplCopyWith<_$S5Error_ConnectionErrorImpl>
      get copyWith => __$$S5Error_ConnectionErrorImplCopyWithImpl<
          _$S5Error_ConnectionErrorImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return connectionError(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return connectionError?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (connectionError != null) {
      return connectionError(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return connectionError(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return connectionError?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (connectionError != null) {
      return connectionError(this);
    }
    return orElse();
  }
}

abstract class S5Error_ConnectionError extends S5Error {
  const factory S5Error_ConnectionError(final String field0) =
      _$S5Error_ConnectionErrorImpl;
  const S5Error_ConnectionError._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_ConnectionErrorImplCopyWith<_$S5Error_ConnectionErrorImpl>
      get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$S5Error_StorageErrorImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_StorageErrorImplCopyWith(_$S5Error_StorageErrorImpl value,
          $Res Function(_$S5Error_StorageErrorImpl) then) =
      __$$S5Error_StorageErrorImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_StorageErrorImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_StorageErrorImpl>
    implements _$$S5Error_StorageErrorImplCopyWith<$Res> {
  __$$S5Error_StorageErrorImplCopyWithImpl(_$S5Error_StorageErrorImpl _value,
      $Res Function(_$S5Error_StorageErrorImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_StorageErrorImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_StorageErrorImpl extends S5Error_StorageError {
  const _$S5Error_StorageErrorImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.storageError(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_StorageErrorImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_StorageErrorImplCopyWith<_$S5Error_StorageErrorImpl>
      get copyWith =>
          __$$S5Error_StorageErrorImplCopyWithImpl<_$S5Error_StorageErrorImpl>(
              this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return storageError(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return storageError?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (storageError != null) {
      return storageError(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return storageError(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return storageError?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (storageError != null) {
      return storageError(this);
    }
    return orElse();
  }
}

abstract class S5Error_StorageError extends S5Error {
  const factory S5Error_StorageError(final String field0) =
      _$S5Error_StorageErrorImpl;
  const S5Error_StorageError._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_StorageErrorImplCopyWith<_$S5Error_StorageErrorImpl>
      get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$S5Error_FileNotFoundImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_FileNotFoundImplCopyWith(_$S5Error_FileNotFoundImpl value,
          $Res Function(_$S5Error_FileNotFoundImpl) then) =
      __$$S5Error_FileNotFoundImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_FileNotFoundImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_FileNotFoundImpl>
    implements _$$S5Error_FileNotFoundImplCopyWith<$Res> {
  __$$S5Error_FileNotFoundImplCopyWithImpl(_$S5Error_FileNotFoundImpl _value,
      $Res Function(_$S5Error_FileNotFoundImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_FileNotFoundImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_FileNotFoundImpl extends S5Error_FileNotFound {
  const _$S5Error_FileNotFoundImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.fileNotFound(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_FileNotFoundImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_FileNotFoundImplCopyWith<_$S5Error_FileNotFoundImpl>
      get copyWith =>
          __$$S5Error_FileNotFoundImplCopyWithImpl<_$S5Error_FileNotFoundImpl>(
              this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return fileNotFound(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return fileNotFound?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (fileNotFound != null) {
      return fileNotFound(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return fileNotFound(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return fileNotFound?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (fileNotFound != null) {
      return fileNotFound(this);
    }
    return orElse();
  }
}

abstract class S5Error_FileNotFound extends S5Error {
  const factory S5Error_FileNotFound(final String field0) =
      _$S5Error_FileNotFoundImpl;
  const S5Error_FileNotFound._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_FileNotFoundImplCopyWith<_$S5Error_FileNotFoundImpl>
      get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$S5Error_CryptoErrorImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_CryptoErrorImplCopyWith(_$S5Error_CryptoErrorImpl value,
          $Res Function(_$S5Error_CryptoErrorImpl) then) =
      __$$S5Error_CryptoErrorImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_CryptoErrorImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_CryptoErrorImpl>
    implements _$$S5Error_CryptoErrorImplCopyWith<$Res> {
  __$$S5Error_CryptoErrorImplCopyWithImpl(_$S5Error_CryptoErrorImpl _value,
      $Res Function(_$S5Error_CryptoErrorImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_CryptoErrorImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_CryptoErrorImpl extends S5Error_CryptoError {
  const _$S5Error_CryptoErrorImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.cryptoError(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_CryptoErrorImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_CryptoErrorImplCopyWith<_$S5Error_CryptoErrorImpl> get copyWith =>
      __$$S5Error_CryptoErrorImplCopyWithImpl<_$S5Error_CryptoErrorImpl>(
          this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return cryptoError(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return cryptoError?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (cryptoError != null) {
      return cryptoError(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return cryptoError(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return cryptoError?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (cryptoError != null) {
      return cryptoError(this);
    }
    return orElse();
  }
}

abstract class S5Error_CryptoError extends S5Error {
  const factory S5Error_CryptoError(final String field0) =
      _$S5Error_CryptoErrorImpl;
  const S5Error_CryptoError._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_CryptoErrorImplCopyWith<_$S5Error_CryptoErrorImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class _$$S5Error_InternalErrorImplCopyWith<$Res>
    implements $S5ErrorCopyWith<$Res> {
  factory _$$S5Error_InternalErrorImplCopyWith(
          _$S5Error_InternalErrorImpl value,
          $Res Function(_$S5Error_InternalErrorImpl) then) =
      __$$S5Error_InternalErrorImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({String field0});
}

/// @nodoc
class __$$S5Error_InternalErrorImplCopyWithImpl<$Res>
    extends _$S5ErrorCopyWithImpl<$Res, _$S5Error_InternalErrorImpl>
    implements _$$S5Error_InternalErrorImplCopyWith<$Res> {
  __$$S5Error_InternalErrorImplCopyWithImpl(_$S5Error_InternalErrorImpl _value,
      $Res Function(_$S5Error_InternalErrorImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? field0 = null,
  }) {
    return _then(_$S5Error_InternalErrorImpl(
      null == field0
          ? _value.field0
          : field0 // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5Error_InternalErrorImpl extends S5Error_InternalError {
  const _$S5Error_InternalErrorImpl(this.field0) : super._();

  @override
  final String field0;

  @override
  String toString() {
    return 'S5Error.internalError(field0: $field0)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5Error_InternalErrorImpl &&
            (identical(other.field0, field0) || other.field0 == field0));
  }

  @override
  int get hashCode => Object.hash(runtimeType, field0);

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5Error_InternalErrorImplCopyWith<_$S5Error_InternalErrorImpl>
      get copyWith => __$$S5Error_InternalErrorImplCopyWithImpl<
          _$S5Error_InternalErrorImpl>(this, _$identity);

  @override
  @optionalTypeArgs
  TResult when<TResult extends Object?>({
    required TResult Function(String field0) invalidInput,
    required TResult Function(String field0) connectionError,
    required TResult Function(String field0) storageError,
    required TResult Function(String field0) fileNotFound,
    required TResult Function(String field0) cryptoError,
    required TResult Function(String field0) internalError,
  }) {
    return internalError(field0);
  }

  @override
  @optionalTypeArgs
  TResult? whenOrNull<TResult extends Object?>({
    TResult? Function(String field0)? invalidInput,
    TResult? Function(String field0)? connectionError,
    TResult? Function(String field0)? storageError,
    TResult? Function(String field0)? fileNotFound,
    TResult? Function(String field0)? cryptoError,
    TResult? Function(String field0)? internalError,
  }) {
    return internalError?.call(field0);
  }

  @override
  @optionalTypeArgs
  TResult maybeWhen<TResult extends Object?>({
    TResult Function(String field0)? invalidInput,
    TResult Function(String field0)? connectionError,
    TResult Function(String field0)? storageError,
    TResult Function(String field0)? fileNotFound,
    TResult Function(String field0)? cryptoError,
    TResult Function(String field0)? internalError,
    required TResult orElse(),
  }) {
    if (internalError != null) {
      return internalError(field0);
    }
    return orElse();
  }

  @override
  @optionalTypeArgs
  TResult map<TResult extends Object?>({
    required TResult Function(S5Error_InvalidInput value) invalidInput,
    required TResult Function(S5Error_ConnectionError value) connectionError,
    required TResult Function(S5Error_StorageError value) storageError,
    required TResult Function(S5Error_FileNotFound value) fileNotFound,
    required TResult Function(S5Error_CryptoError value) cryptoError,
    required TResult Function(S5Error_InternalError value) internalError,
  }) {
    return internalError(this);
  }

  @override
  @optionalTypeArgs
  TResult? mapOrNull<TResult extends Object?>({
    TResult? Function(S5Error_InvalidInput value)? invalidInput,
    TResult? Function(S5Error_ConnectionError value)? connectionError,
    TResult? Function(S5Error_StorageError value)? storageError,
    TResult? Function(S5Error_FileNotFound value)? fileNotFound,
    TResult? Function(S5Error_CryptoError value)? cryptoError,
    TResult? Function(S5Error_InternalError value)? internalError,
  }) {
    return internalError?.call(this);
  }

  @override
  @optionalTypeArgs
  TResult maybeMap<TResult extends Object?>({
    TResult Function(S5Error_InvalidInput value)? invalidInput,
    TResult Function(S5Error_ConnectionError value)? connectionError,
    TResult Function(S5Error_StorageError value)? storageError,
    TResult Function(S5Error_FileNotFound value)? fileNotFound,
    TResult Function(S5Error_CryptoError value)? cryptoError,
    TResult Function(S5Error_InternalError value)? internalError,
    required TResult orElse(),
  }) {
    if (internalError != null) {
      return internalError(this);
    }
    return orElse();
  }
}

abstract class S5Error_InternalError extends S5Error {
  const factory S5Error_InternalError(final String field0) =
      _$S5Error_InternalErrorImpl;
  const S5Error_InternalError._() : super._();

  @override
  String get field0;

  /// Create a copy of S5Error
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5Error_InternalErrorImplCopyWith<_$S5Error_InternalErrorImpl>
      get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
mixin _$S5Keys {
  String get rootSecretHex => throw _privateConstructorUsedError;
  String get publicKeyHex => throw _privateConstructorUsedError;
  String get encryptionKeyHex => throw _privateConstructorUsedError;
  String get signingKeyHex => throw _privateConstructorUsedError;
  String get irohSecretKeyHex => throw _privateConstructorUsedError;

  /// Create a copy of S5Keys
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $S5KeysCopyWith<S5Keys> get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $S5KeysCopyWith<$Res> {
  factory $S5KeysCopyWith(S5Keys value, $Res Function(S5Keys) then) =
      _$S5KeysCopyWithImpl<$Res, S5Keys>;
  @useResult
  $Res call(
      {String rootSecretHex,
      String publicKeyHex,
      String encryptionKeyHex,
      String signingKeyHex,
      String irohSecretKeyHex});
}

/// @nodoc
class _$S5KeysCopyWithImpl<$Res, $Val extends S5Keys>
    implements $S5KeysCopyWith<$Res> {
  _$S5KeysCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of S5Keys
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? rootSecretHex = null,
    Object? publicKeyHex = null,
    Object? encryptionKeyHex = null,
    Object? signingKeyHex = null,
    Object? irohSecretKeyHex = null,
  }) {
    return _then(_value.copyWith(
      rootSecretHex: null == rootSecretHex
          ? _value.rootSecretHex
          : rootSecretHex // ignore: cast_nullable_to_non_nullable
              as String,
      publicKeyHex: null == publicKeyHex
          ? _value.publicKeyHex
          : publicKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      encryptionKeyHex: null == encryptionKeyHex
          ? _value.encryptionKeyHex
          : encryptionKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      signingKeyHex: null == signingKeyHex
          ? _value.signingKeyHex
          : signingKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      irohSecretKeyHex: null == irohSecretKeyHex
          ? _value.irohSecretKeyHex
          : irohSecretKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$S5KeysImplCopyWith<$Res> implements $S5KeysCopyWith<$Res> {
  factory _$$S5KeysImplCopyWith(
          _$S5KeysImpl value, $Res Function(_$S5KeysImpl) then) =
      __$$S5KeysImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call(
      {String rootSecretHex,
      String publicKeyHex,
      String encryptionKeyHex,
      String signingKeyHex,
      String irohSecretKeyHex});
}

/// @nodoc
class __$$S5KeysImplCopyWithImpl<$Res>
    extends _$S5KeysCopyWithImpl<$Res, _$S5KeysImpl>
    implements _$$S5KeysImplCopyWith<$Res> {
  __$$S5KeysImplCopyWithImpl(
      _$S5KeysImpl _value, $Res Function(_$S5KeysImpl) _then)
      : super(_value, _then);

  /// Create a copy of S5Keys
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? rootSecretHex = null,
    Object? publicKeyHex = null,
    Object? encryptionKeyHex = null,
    Object? signingKeyHex = null,
    Object? irohSecretKeyHex = null,
  }) {
    return _then(_$S5KeysImpl(
      rootSecretHex: null == rootSecretHex
          ? _value.rootSecretHex
          : rootSecretHex // ignore: cast_nullable_to_non_nullable
              as String,
      publicKeyHex: null == publicKeyHex
          ? _value.publicKeyHex
          : publicKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      encryptionKeyHex: null == encryptionKeyHex
          ? _value.encryptionKeyHex
          : encryptionKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      signingKeyHex: null == signingKeyHex
          ? _value.signingKeyHex
          : signingKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
      irohSecretKeyHex: null == irohSecretKeyHex
          ? _value.irohSecretKeyHex
          : irohSecretKeyHex // ignore: cast_nullable_to_non_nullable
              as String,
    ));
  }
}

/// @nodoc

class _$S5KeysImpl implements _S5Keys {
  const _$S5KeysImpl(
      {required this.rootSecretHex,
      required this.publicKeyHex,
      required this.encryptionKeyHex,
      required this.signingKeyHex,
      required this.irohSecretKeyHex});

  @override
  final String rootSecretHex;
  @override
  final String publicKeyHex;
  @override
  final String encryptionKeyHex;
  @override
  final String signingKeyHex;
  @override
  final String irohSecretKeyHex;

  @override
  String toString() {
    return 'S5Keys(rootSecretHex: $rootSecretHex, publicKeyHex: $publicKeyHex, encryptionKeyHex: $encryptionKeyHex, signingKeyHex: $signingKeyHex, irohSecretKeyHex: $irohSecretKeyHex)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$S5KeysImpl &&
            (identical(other.rootSecretHex, rootSecretHex) ||
                other.rootSecretHex == rootSecretHex) &&
            (identical(other.publicKeyHex, publicKeyHex) ||
                other.publicKeyHex == publicKeyHex) &&
            (identical(other.encryptionKeyHex, encryptionKeyHex) ||
                other.encryptionKeyHex == encryptionKeyHex) &&
            (identical(other.signingKeyHex, signingKeyHex) ||
                other.signingKeyHex == signingKeyHex) &&
            (identical(other.irohSecretKeyHex, irohSecretKeyHex) ||
                other.irohSecretKeyHex == irohSecretKeyHex));
  }

  @override
  int get hashCode => Object.hash(runtimeType, rootSecretHex, publicKeyHex,
      encryptionKeyHex, signingKeyHex, irohSecretKeyHex);

  /// Create a copy of S5Keys
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$S5KeysImplCopyWith<_$S5KeysImpl> get copyWith =>
      __$$S5KeysImplCopyWithImpl<_$S5KeysImpl>(this, _$identity);
}

abstract class _S5Keys implements S5Keys {
  const factory _S5Keys(
      {required final String rootSecretHex,
      required final String publicKeyHex,
      required final String encryptionKeyHex,
      required final String signingKeyHex,
      required final String irohSecretKeyHex}) = _$S5KeysImpl;

  @override
  String get rootSecretHex;
  @override
  String get publicKeyHex;
  @override
  String get encryptionKeyHex;
  @override
  String get signingKeyHex;
  @override
  String get irohSecretKeyHex;

  /// Create a copy of S5Keys
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$S5KeysImplCopyWith<_$S5KeysImpl> get copyWith =>
      throw _privateConstructorUsedError;
}
