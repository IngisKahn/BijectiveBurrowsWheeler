using System;
using System.Linq;

namespace BijectiveBurrowsWheeler
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using BenchmarkIt;

    class Program
    {
        private static readonly short[] lengthFrequency =
            { 1, 10, 60, 260, 520, 850, 1220, 1400, 1400, 1260, 1010, 750, 520, 320, 200, 100, 60, 30, 20, 10, 10, 1 };
        private static readonly short[] firstFrequency =
        {
            11682, 4434, 5238, 3174, 2799, 4027, 1642, 4200, 7294, 511, 856, 2415, 3826, 2284, 7631, 4319, 222, 2826, 6686, 15978, 1183, 824, 5497, 45, 763, 45
        };

        private static readonly short[,] pairFrequency =
        {
            {1, 20, 33, 52, 0, 12, 18, 5, 39, 1, 12, 57, 26, 181, 1, 20, 1, 75, 95, 104, 9, 20, 13, 1, 26, 1      },
            {11, 1, 0, 0, 47, 0, 0, 0, 6, 1, 0, 17, 0, 0, 19, 0, 0, 11, 2, 1, 21, 0, 0, 0, 11, 0                  },
            {31, 0, 4, 0, 38, 0, 0, 38, 10, 0, 18, 9, 0, 0, 45, 0, 1, 11, 1, 15, 7, 0, 0, 0, 1, 0                 },
            {48, 20, 9, 13, 57, 11, 7, 25, 50, 3, 1, 11, 14, 16, 41, 6, 0, 14, 35, 56, 10, 2, 19, 0, 10, 0        },
            {110, 23, 45, 126, 48, 30, 15, 33, 41, 3, 5, 55, 47, 111, 33, 28, 2, 169, 115, 83, 6, 24, 50, 9, 26, 0},
            {25, 2, 3, 2, 20, 11, 1, 8, 23, 1, 0, 8, 5, 1, 40, 2, 0, 16, 5, 37, 8, 0, 3, 0, 2, 0                  },
            {24, 3, 2, 2, 28, 3, 4, 35, 18, 1, 0, 7, 3, 4, 23, 1, 0, 12, 9, 16, 7, 0, 5, 0, 1, 0                  },
            {114, 2, 2, 1, 302, 2, 1, 6, 97, 0, 0, 2, 3, 1, 49, 1, 0, 8, 5, 32, 8, 0, 4, 0, 4, 0                  },
            {10, 5, 32, 33, 23, 17, 25, 6, 1, 1, 8, 37, 37, 179, 24, 6, 0, 27, 86, 93, 1, 14, 7, 2, 0, 2          },
            {2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0                         },
            {6, 1, 1, 1, 29, 1, 0, 2, 14, 0, 0, 2, 1, 9, 4, 0, 0, 0, 5, 4, 1, 0, 2, 0, 2, 0                       },
            {40, 3, 2, 36, 64, 10, 1, 4, 47, 0, 3, 56, 4, 2, 41, 3, 0, 2, 11, 15, 8, 3, 5, 0, 31, 0               },
            {44, 7, 1, 1, 68, 2, 1, 3, 25, 0, 0, 1, 5, 2, 29, 11, 0, 3, 10, 9, 8, 0, 4, 0, 18, 0                  },
            {40, 7, 25, 146, 66, 8, 92, 16, 33, 2, 8, 9, 7, 8, 60, 4, 1, 3, 33, 106, 6, 2, 12, 0, 11, 0           },
            {16, 12, 13, 18, 5, 80, 7, 11, 12, 1, 13, 26, 48, 106, 36, 15, 0, 84, 28, 57, 115, 12, 46, 0, 5, 1    },
            {23, 1, 0, 0, 30, 1, 0, 3, 12, 0, 0, 15, 1, 0, 21, 10, 0, 18, 5, 11, 6, 0, 1, 0, 1, 0                 },
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0                         },
            {50, 7, 10, 20, 133, 8, 10, 12, 50, 1, 8, 10, 14, 16, 55, 6, 0, 14, 37, 42, 12, 4, 11, 0, 21, 0       },
            {67, 11, 17, 7, 74, 11, 4, 50, 49, 2, 6, 13, 12, 10, 57, 20, 2, 4, 43, 109, 20, 2, 24, 0, 4, 0        },
            {59, 10, 11, 7, 75, 9, 3, 330, 76, 1, 2, 17, 11, 7, 115, 4, 0, 28, 34, 56, 17, 1, 31, 0, 16, 0        },
            {7, 5, 12, 7, 7, 2, 14, 2, 8, 0, 1, 34, 8, 36, 1, 16, 0, 44, 35, 48, 0, 0, 2, 0, 1, 0                 },
            {5, 0, 0, 0, 65, 0, 0, 0, 11, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0                       },
            {66, 1, 1, 2, 39, 1, 0, 44, 39, 0, 0, 2, 1, 12, 29, 0, 0, 3, 4, 4, 1, 0, 2, 0, 1, 0                   },
            {1, 0, 2, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0                         },
            {18, 7, 6, 6, 14, 7, 3, 10, 11, 1, 1, 4, 6, 3, 36, 4, 0, 3, 19, 20, 1, 1, 12, 0, 2, 0                 },
            {1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0                         }
        };

        static void Main(string[] args)
        {
            // setup the word generator
            const int lengthSkip = 2;
            var lengthRanges = new short[Program.lengthFrequency.Length - lengthSkip];
            var lengthMax = Program.lengthFrequency.Skip(lengthSkip).Select(x => (int)x).Sum();
            for (var x = 1; x < lengthRanges.Length - lengthSkip; x++)
                lengthRanges[x] = (short)(lengthRanges[x - 1] + Program.lengthFrequency[x - 1 + lengthSkip]);
            var firstRanges = new int[26];
            var firstMax = Program.firstFrequency.Select(x => (int)x).Sum();
            for (var x = 1; x < 26; x++)
                firstRanges[x] = firstRanges[x - 1] + Program.firstFrequency[x - 1];
            var pairRanges = new short[26, 26];
            var pairMaxes = new short[26];
            for (var x = 0; x < 26; x++)
            {
                for (var y = 1; y < 26; y++)
                {
                    pairMaxes[x] += Program.pairFrequency[x, y - 1];
                    pairRanges[x, y] = (short)(pairRanges[x, y - 1] + Program.pairFrequency[x, y - 1]);
                }
                pairMaxes[x] += Program.pairFrequency[x, 25];
            }

            string WordGenerator(int length, Random rnd)
            {
                var sb = new StringBuilder(length + 22);

                while (sb.Length < length)
                {
                    if (sb.Length > 0)
                        sb.Append(' ');
                    var clink = rnd.Next(lengthMax);
                    var lengthIndex = 0;
                    while (lengthIndex < lengthRanges.Length && lengthRanges[lengthIndex] < clink)
                        lengthIndex++;
                    var c = rnd.Next(firstMax);
                    var letterIndex = 0;
                    while (letterIndex < 25 && firstRanges[letterIndex + 1] < c)
                        letterIndex++;
                    sb.Append((char)('a' + letterIndex));

                    while (--lengthIndex > 0)
                    {
                        c = rnd.Next(pairMaxes[(int)letterIndex]);
                        var nextLetterIndex = 0;
                        while (nextLetterIndex < 25 && pairRanges[letterIndex, nextLetterIndex + 1] < c)
                            nextLetterIndex++;
                        sb.Append((char)('a' + nextLetterIndex));
                        letterIndex = nextLetterIndex;
                    }
                }


                return sb.ToString();
            }
            var rnd1 = new Random(222);
            var rnd2 = new Random(222);


            Benchmark.This("10L", () => { Program.Transform(WordGenerator(10, rnd1)); })
                .Against.This("10M", () => { Program.TransformMultiThreaded(WordGenerator(10, rnd2)); })
                .WithWarmup(100)
                .For(2).Seconds().PrintComparison();

            Benchmark.This("100L", () => { Program.Transform(WordGenerator(100, rnd1)); })
                .Against.This("100M", () => { Program.TransformMultiThreaded(WordGenerator(100, rnd2)); })
                .WithWarmup(100)
                .For(3).Seconds().PrintComparison();

            Benchmark.This("1000L", () => { Program.Transform(WordGenerator(1000, rnd1)); })
                .Against.This("1000M", () => { Program.TransformMultiThreaded(WordGenerator(1000, rnd2)); })
                .WithWarmup(100)
                .For(4).Seconds().PrintComparison();

            Benchmark.This("10000L", () => { Program.Transform(WordGenerator(10000, rnd1)); })
                .Against.This("10000M", () => { Program.TransformMultiThreaded(WordGenerator(10000, rnd2)); })
                .WithWarmup(100)
                .For(6).Seconds().PrintComparison();
        }

        private static int work;
        public static string TransformMultiThreaded(string source)
        {
            Program.work = 1;
            var processQueue = new ManualResetEventSlim(false);
            var finishedProcessing = new ManualResetEventSlim(false);
            var mergeQueue = new ConcurrentQueue<(int, int)[]>();
            var factorData = new List<(int, int)>();

            void SortFactor(int index, int start, int length)
            {
                var sorted = Program.SortBucket(source, start, length, Enumerable.Range(0, length).ToArray(), 1)
                    .Select(v => (index, v)).ToArray();

                mergeQueue.Enqueue(sorted);

                processQueue.Set();
            }

            void ProcessSorted()
            {
                void MergeTwo((int, int)[] a, (int, int)[] b)
                {
                    var leftCount = a.Length;
                    var rightCount = b.Length;
                    var totalCount = leftCount + rightCount;

                    var merged = new (int, int)[totalCount];
                    var leftIndex = 0;
                    var rightIndex = 0;
                    var resultIndex = 0;

                    while (leftIndex < leftCount && rightIndex < rightCount)
                        merged[resultIndex++] = IsRotationLesser(factorData[b[rightIndex].Item1], b[rightIndex].Item2, factorData[a[leftIndex].Item1], a[leftIndex].Item2)
                            ? b[rightIndex++]
                            : a[leftIndex++];

                    if (leftIndex < leftCount)
                        for (; leftIndex < leftCount; leftIndex++)
                            merged[resultIndex++] = a[leftIndex];
                    else if (rightIndex < rightCount)
                        for (; rightIndex < rightCount; rightIndex++)
                            merged[resultIndex++] = b[rightIndex];

                    mergeQueue.Enqueue(merged);


                    bool IsRotationLesser((int Start, int Length) leftFactor, int leftRotation, (int Start, int Length) rightFactor, int rightRotation)
                    {
                        for (var k = 0; k < leftFactor.Length * rightFactor.Length; k++)
                        {
                            var leftChar = source[leftFactor.Start + leftRotation];
                            var rightChar = source[rightFactor.Start + rightRotation];
                            if (leftChar < rightChar)
                                return true;
                            if (leftChar > rightChar)
                                return false;
                            if (++leftRotation == leftFactor.Length)
                                leftRotation = 0;
                            if (++rightRotation == rightFactor.Length)
                                rightRotation = 0;
                        }

                        return false;
                    }

                    Interlocked.Decrement(ref Program.work);

                    processQueue.Set();
                }

                for (; ; )
                {
                    processQueue.Wait();
                    processQueue.Reset();

                    if (Program.work == 1 && !mergeQueue.IsEmpty)
                    {
                        finishedProcessing.Set();
                        return;
                    }

                    if (mergeQueue.Count < 2)
                        continue;

                    mergeQueue.TryDequeue(out var a);
                    mergeQueue.TryDequeue(out var b);
                    Task.Run(() => MergeTwo(a, b));
                }
            }

            Task.Run(ProcessSorted);

            var currentTermIndex = 0;
            var startIndex = 0;
            var endIndex = 1;
            var currentFactor = 0;

            for (; currentTermIndex < source.Length;)
                if (endIndex + currentTermIndex < source.Length)
                {
                    var c = source[startIndex + currentTermIndex].CompareTo(source[endIndex + currentTermIndex]);
                    if (c > 0)
                    {
                        var start = currentTermIndex;
                        var length = endIndex - startIndex;
                        factorData.Add((start, length));
                        var index = currentFactor;
                        Interlocked.Increment(ref Program.work);

                        Task.Run(() => SortFactor(index, start, length));
                        currentFactor++;
                        currentTermIndex += endIndex - startIndex;
                        endIndex = 1;
                        startIndex = 0;
                    }
                    else
                    {
                        endIndex++;
                        startIndex = c < 0 ? 0 : startIndex + 1;
                    }
                }
                else
                {
                    var start = currentTermIndex;
                    var length = Math.Min(endIndex - startIndex, source.Length - currentTermIndex);
                    factorData.Add((start, length));
                    var index = currentFactor;
                    Interlocked.Increment(ref Program.work);

                    Task.Run(() => SortFactor(index, start, length));
                    currentFactor++;
                    currentTermIndex += endIndex - startIndex;
                }

            Interlocked.Decrement(ref Program.work);

            processQueue.Set();
            finishedProcessing.Wait();
            mergeQueue.TryDequeue(out var result);

            var output = new char[source.Length];
            for (var i = 0; i < result.Length; i++)
            {
                var (factorIndex, rotationIndex) = result[i];
                if (--rotationIndex < 0) // wrap
                    rotationIndex += factorData[factorIndex].Item2;
                output[i] = source[factorData[factorIndex].Item1 + rotationIndex];
            }
            return new string(output);
        }
        public static IEnumerable<int> SortBucket(string s, int start, int length, IList<int> bucket, int order)
        {
            var keyToPositionMap = new Dictionary<string, List<int>>(bucket.Count);
            foreach (var i in bucket)
            {
                var key = s[(start + i + order / 2)..(start + Math.Min(i + order, length))];
                if (!keyToPositionMap.TryGetValue(key, out var de))
                    keyToPositionMap[key] = de = new List<int>();
                de.Add(i);
            }

            var result = new List<int>();
            foreach (var v in keyToPositionMap.OrderBy(kvp => kvp.Key, StringComparer.Ordinal).Select(de => de.Value))
                if (v.Count > 1)
                    result.AddRange(Program.SortBucket(s, start, length, v, 2 * order));
                else
                    result.Add(v[0]);

            return result;
        }

        public static string[] LyndonFactor(string s)
        {
            var output = new List<string>();
            var currentTermIndex = 0;
            var startIndex = 0;
            var endIndex = 1;
            for (; currentTermIndex < s.Length;)
                if (endIndex + currentTermIndex < s.Length)
                {
                    var c = s[startIndex + currentTermIndex].CompareTo(s[endIndex + currentTermIndex]);
                    if (c > 0)
                    {
                        output.Add(s.Substring(currentTermIndex, endIndex - startIndex));
                        currentTermIndex += endIndex - startIndex;
                        endIndex = 1;
                        startIndex = 0;
                    }
                    else
                    {
                        endIndex++;
                        startIndex = c < 0 ? 0 : startIndex + 1;
                    }
                }
                else
                {
                    output.Add(s.Substring(currentTermIndex, Math.Min(endIndex - startIndex, s.Length - currentTermIndex)));
                    currentTermIndex += endIndex - startIndex;
                }

            return output.ToArray();
        }

        public static string Restore(string source)
        {
            // how many times each character was used
            var counts = new Dictionary<char, int>();
            foreach (var c in source)
            {
                counts.TryGetValue(c, out var count);
                counts[c] = count + 1;
            }

            // convert to a range of usage
            var orderedKeys = counts.Keys.OrderBy(k => k).ToArray();
            var last = counts[orderedKeys[0]];
            counts[orderedKeys[0]] = 0;
            foreach (var c in orderedKeys[1..])
            {
                var temp = last;
                last += counts[c];
                counts[c] = temp;
            }

            // assign a unique index to each character
            var sourceIds = new int[source.Length];
            for (var i = 0; i < source.Length; i++)
                sourceIds[i] = counts[source[i]]++;

            // trace each factor to output
            var output = new char[source.Length];
            var outputPosition = source.Length - 1;
            for (var sourceIndex = 0; sourceIndex < source.Length; sourceIndex++)
            {
                var workingIndex = sourceIndex;
                while (sourceIds[workingIndex] != -1)
                {
                    output[outputPosition--] = source[workingIndex];
                    var nextIndex = sourceIds[workingIndex];
                    sourceIds[workingIndex] = -1;
                    workingIndex = nextIndex;
                }
            }
            return new string(output);
        }

        public static string Transform(string s)
        {
            var output = new char[s.Length];

            var factors = Program.LyndonFactor(s);

            var sorted = Program.SortRotations(factors);
            for (var i = 0; i < sorted.Length; i++)
            {
                var (factorIndex, rotationIndex) = sorted[i];
                if (--rotationIndex < 0) // wrap
                    rotationIndex += factors[factorIndex].Length;
                output[i] = factors[factorIndex][rotationIndex];
            }
            return new string(output);
        }

        private static (int w, int r)[] SortRotations(string[] factors) =>
            Program.MergeRotations(factors, factors.Select((t, x) => Program.SortSubstrings(t).Select(s => (x, s)).ToArray()));

        public static IEnumerable<int> SortSubstrings(string s) =>
            Program.SortBucket(s, Enumerable.Range(0, s.Length).ToArray(), 1);

        public static IEnumerable<int> SortBucket(string s, IList<int> bucket, int order)
        {
            var keyToPositionMap = new Dictionary<string, List<int>>(bucket.Count);
            foreach (var i in bucket)
            {
                var key = s[(i + order / 2)..Math.Min(i + order, s.Length)];
                if (!keyToPositionMap.TryGetValue(key, out var de))
                    keyToPositionMap[key] = de = new List<int>();
                de.Add(i);
            }

            var result = new List<int>();
            foreach (var v in keyToPositionMap.OrderBy(kvp => kvp.Key, StringComparer.Ordinal).Select(de => de.Value))
                if (v.Count > 1)
                    result.AddRange(Program.SortBucket(s, v, 2 * order));
                else
                    result.Add(v[0]);

            return result;
        }

        public static (int, int)[] MergeRotations(string[] factors, IEnumerable<(int, int)[]> rotations) =>
            rotations.Aggregate(new (int, int)[0], (current, rotation) => Program.MergeRotation(factors, current, rotation));

        public static (int w, int r)[] MergeRotation(string[] factors, (int, int)[] leftRotations, (int, int)[] rightRotations)
        {
            var leftCount = leftRotations.Length;
            var rightCount = rightRotations.Length;
            var totalCount = leftCount + rightCount;

            var output = new (int, int)[totalCount];
            var leftIndex = 0;
            var rightIndex = 0;
            var resultIndex = 0;

            while (leftIndex < leftCount && rightIndex < rightCount)
                output[resultIndex++] = Program.IsRotationLesser(factors, rightRotations[rightIndex], leftRotations[leftIndex])
                    ? rightRotations[rightIndex++]
                    : leftRotations[leftIndex++];

            if (leftIndex < leftCount)
                for (; leftIndex < leftCount; leftIndex++)
                    output[resultIndex++] = leftRotations[leftIndex];
            else if (rightIndex < rightCount)
                for (; rightIndex < rightCount; rightIndex++)
                    output[resultIndex++] = rightRotations[rightIndex];

            return output;
        }

        public static bool IsRotationLesser(string[] factors, (int, int) left, (int, int) right)
        {
            var (leftFactorIndex, leftFactorCharacterIndex) = left;
            var leftFactor = factors[leftFactorIndex];
            var leftFactorLength = leftFactor.Length;

            var (rightFactorIndex, rightFactorCharacterIndex) = right;
            var rightFactor = factors[rightFactorIndex];
            var rightFactorLength = rightFactor.Length;

            for (var k = 0; k < leftFactorLength * rightFactorLength; k++)
            {
                if (leftFactor[leftFactorCharacterIndex] < rightFactor[rightFactorCharacterIndex])
                    return true;
                if (leftFactor[leftFactorCharacterIndex] > rightFactor[rightFactorCharacterIndex])
                    return false;
                if (++leftFactorCharacterIndex == leftFactorLength)
                    leftFactorCharacterIndex = 0;
                if (++rightFactorCharacterIndex == rightFactorLength)
                    rightFactorCharacterIndex = 0;
            }

            return false;
        }
    }
}
