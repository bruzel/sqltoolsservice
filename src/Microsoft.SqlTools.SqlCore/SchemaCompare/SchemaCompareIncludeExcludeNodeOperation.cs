//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using Microsoft.SqlServer.Dac.Compare;
using Microsoft.SqlTools.SqlCore.SchemaCompare.Contracts;
using Microsoft.SqlTools.Utility;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Microsoft.SqlTools.SqlCore.SchemaCompare
{
    /// <summary>
    /// Host-agnostic schema compare include/exclude node operation.
    /// </summary>
    public class SchemaCompareIncludeExcludeNodeOperation : IDisposable
    {
        private CancellationTokenSource cancellation = new CancellationTokenSource();
        private bool disposed = false;

        /// <summary>
        /// Gets the unique identifier for this operation.
        /// </summary>
        public string OperationId { get; private set; }

        /// <summary>
        /// Gets the parameters for the include/exclude node operation.
        /// </summary>
        public SchemaCompareNodeParams Parameters { get; }

        protected CancellationToken CancellationToken { get { return this.cancellation.Token; } }

        /// <summary>
        /// The error message if the operation failed.
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// The schema comparison result to include/exclude a node from.
        /// </summary>
        public SchemaComparisonResult ComparisonResult { get; set; }

        /// <summary>
        /// Whether the include/exclude operation succeeded.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// List of diff entries affected by the include/exclude operation.
        /// </summary>
        public List<DiffEntry> AffectedDependencies;

        /// <summary>
        /// List of diff entries blocking an exclude operation.
        /// </summary>
        public List<DiffEntry> BlockingDependencies;

        /// <summary>
        /// Initializes a new include/exclude node operation with parameters and comparison result.
        /// </summary>
        public SchemaCompareIncludeExcludeNodeOperation(SchemaCompareNodeParams parameters, SchemaComparisonResult comparisonResult)
        {
            Validate.IsNotNull("parameters", parameters);
            this.Parameters = parameters;
            Validate.IsNotNull("comparisonResult", comparisonResult);
            this.ComparisonResult = comparisonResult;
        }

        /// <summary>
        /// Exclude will return false if included dependencies are found.
        /// Include will also include dependencies that need to be included.
        /// </summary>
        public void Execute()
        {
            this.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                var index = new Dictionary<DifferenceKey, SchemaDifference>();
                BuildDifferenceIndex(this.ComparisonResult.Differences, index);

                DifferenceKey lookupKey = GetKeyFromDiffEntry(this.Parameters.DiffEntry);
                if (!index.TryGetValue(lookupKey, out SchemaDifference node))
                {
                    throw new InvalidOperationException("Schema compare include/exclude node not found.");
                }

                this.Success = this.Parameters.IncludeRequest ? this.ComparisonResult.Include(node) : this.ComparisonResult.Exclude(node);

                if (this.Parameters.IncludeRequest)
                {
                    IEnumerable<SchemaDifference> affectedDependencies = this.ComparisonResult.GetIncludeDependencies(node);
                    this.AffectedDependencies = affectedDependencies.Select(difference => SchemaCompareUtils.CreateDiffEntry(difference: difference, parent: null, schemaComparisonResult: this.ComparisonResult)).ToList();
                }
                else
                {
                    if (this.Success)
                    {
                        IEnumerable<SchemaDifference> affectedDependencies = this.ComparisonResult.GetIncludeDependencies(node);
                        this.AffectedDependencies = affectedDependencies.Select(difference => SchemaCompareUtils.CreateDiffEntry(difference: difference, parent: null, schemaComparisonResult: this.ComparisonResult)).ToList();
                    }
                    else
                    {
                        IEnumerable<SchemaDifference> blockingDependencies = this.ComparisonResult.GetExcludeDependencies(node);
                        blockingDependencies = blockingDependencies.Where(difference => difference.Included == node.Included);
                        this.BlockingDependencies = blockingDependencies.Select(difference => SchemaCompareUtils.CreateDiffEntry(difference: difference, parent: null, schemaComparisonResult: this.ComparisonResult)).ToList();
                    }
                }
            }
            catch (Exception e)
            {
                ErrorMessage = e.Message;
                Logger.Error(string.Format("Schema compare include/exclude operation {0} failed with exception {1}", this.OperationId, e.Message));
                throw;
            }
        }

        private const string NamePartsSeparator = ",";

        private static void BuildDifferenceIndex(IEnumerable<SchemaDifference> differences, Dictionary<DifferenceKey, SchemaDifference> index)
        {
            foreach (var difference in differences)
            {
                var key = GetKeyFromDifference(difference);
                if (!index.ContainsKey(key))
                {
                    index.Add(key, difference);
                }
                BuildDifferenceIndex(difference.Children, index);
            }
        }

        private static DifferenceKey GetKeyFromDifference(SchemaDifference difference)
        {
            string sourceObjectType = null;
            string sourceValue = string.Empty;
            if (difference.SourceObject != null)
            {
                sourceObjectType = new SchemaComparisonExcludedObjectId(difference.SourceObject.ObjectType, difference.SourceObject.Name).TypeName;
                sourceValue = string.Join(NamePartsSeparator, difference.SourceObject.Name.Parts);
            }

            string targetObjectType = null;
            string targetValue = string.Empty;
            if (difference.TargetObject != null)
            {
                targetObjectType = new SchemaComparisonExcludedObjectId(difference.TargetObject.ObjectType, difference.TargetObject.Name).TypeName;
                targetValue = string.Join(NamePartsSeparator, difference.TargetObject.Name.Parts);
            }

            return new DifferenceKey(
                difference.Name,
                difference.UpdateAction,
                difference.DifferenceType,
                sourceObjectType,
                sourceValue,
                targetObjectType,
                targetValue);
        }

        private static DifferenceKey GetKeyFromDiffEntry(DiffEntry diffEntry)
        {
            string sourceValue = diffEntry.SourceValue != null ? string.Join(NamePartsSeparator, diffEntry.SourceValue) : string.Empty;
            string targetValue = diffEntry.TargetValue != null ? string.Join(NamePartsSeparator, diffEntry.TargetValue) : string.Empty;

            return new DifferenceKey(
                diffEntry.Name,
                diffEntry.UpdateAction,
                diffEntry.DifferenceType,
                diffEntry.SourceObjectType,
                sourceValue,
                diffEntry.TargetObjectType,
                targetValue);
        }

        private readonly struct DifferenceKey : IEquatable<DifferenceKey>
        {
            public readonly string Name;
            public readonly SchemaUpdateAction UpdateAction;
            public readonly SchemaDifferenceType DifferenceType;
            public readonly string SourceObjectType;
            public readonly string SourceValue;
            public readonly string TargetObjectType;
            public readonly string TargetValue;

            public DifferenceKey(string name, SchemaUpdateAction updateAction, SchemaDifferenceType differenceType,
                string sourceObjectType, string sourceValue, string targetObjectType, string targetValue)
            {
                Name = name;
                UpdateAction = updateAction;
                DifferenceType = differenceType;
                SourceObjectType = sourceObjectType;
                SourceValue = sourceValue;
                TargetObjectType = targetObjectType;
                TargetValue = targetValue;
            }

            public bool Equals(DifferenceKey other) =>
                string.Equals(Name, other.Name, StringComparison.Ordinal) &&
                UpdateAction == other.UpdateAction &&
                DifferenceType == other.DifferenceType &&
                string.Equals(SourceObjectType, other.SourceObjectType, StringComparison.Ordinal) &&
                string.Equals(SourceValue, other.SourceValue, StringComparison.Ordinal) &&
                string.Equals(TargetObjectType, other.TargetObjectType, StringComparison.Ordinal) &&
                string.Equals(TargetValue, other.TargetValue, StringComparison.Ordinal);

            public override bool Equals(object obj) => obj is DifferenceKey other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    int hash = 17;
                    hash = hash * 31 + (Name != null ? StringComparer.Ordinal.GetHashCode(Name) : 0);
                    hash = hash * 31 + UpdateAction.GetHashCode();
                    hash = hash * 31 + DifferenceType.GetHashCode();
                    hash = hash * 31 + (SourceObjectType != null ? StringComparer.Ordinal.GetHashCode(SourceObjectType) : 0);
                    hash = hash * 31 + (SourceValue != null ? StringComparer.Ordinal.GetHashCode(SourceValue) : 0);
                    hash = hash * 31 + (TargetObjectType != null ? StringComparer.Ordinal.GetHashCode(TargetObjectType) : 0);
                    hash = hash * 31 + (TargetValue != null ? StringComparer.Ordinal.GetHashCode(TargetValue) : 0);
                    return hash;
                }
            }
        }

        /// <summary>
        /// Cancels the running operation.
        /// </summary>
        public void Cancel()
        {
        }

        /// <summary>
        /// Disposes the operation and cancels any pending work.
        /// </summary>
        public void Dispose()
        {
            if (!disposed)
            {
                this.Cancel();
                disposed = true;
            }
        }
    }
}
