#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert NCBI-style GFF3:
    gene -> CDS
to a VEP-friendlier structure:
    gene -> mRNA -> CDS

It only:
1. inserts one mRNA row for each gene
2. rewrites CDS Parent from gene ID to transcript ID

It does NOT change:
- coordinates
- strand
- phase
- seqid
- gene names / product names

Usage:
    python fix_gff_for_vep.py input.gff3 output.gff3
"""

import sys


def parse_attrs(attr_str: str):
    attrs = []
    d = {}
    if not attr_str.strip():
        return attrs, d
    for item in attr_str.strip().split(";"):
        if not item:
            continue
        if "=" in item:
            k, v = item.split("=", 1)
            attrs.append((k, v))
            d[k] = v
        else:
            attrs.append((item, ""))
            d[item] = ""
    return attrs, d


def build_attr_str(attr_pairs):
    parts = []
    for k, v in attr_pairs:
        if v == "":
            parts.append(k)
        else:
            parts.append(f"{k}={v}")
    return ";".join(parts)


def replace_or_add_attr(attr_pairs, key, value):
    found = False
    new_pairs = []
    for k, v in attr_pairs:
        if k == key:
            new_pairs.append((k, value))
            found = True
        else:
            new_pairs.append((k, v))
    if not found:
        new_pairs.append((key, value))
    return new_pairs


def main():
    if len(sys.argv) != 3:
        print("Usage: python fix_gff_for_vep.py input.gff3 output.gff3", file=sys.stderr)
        sys.exit(1)

    input_gff = sys.argv[1]
    output_gff = sys.argv[2]

    # gene_id -> transcript_id
    gene_to_tx = {}

    with open(input_gff, "r", encoding="utf-8") as fin, open(output_gff, "w", encoding="utf-8") as fout:
        for raw_line in fin:
            line = raw_line.rstrip("\n")

            if not line:
                fout.write("\n")
                continue

            if line.startswith("#"):
                fout.write(raw_line)
                continue

            cols = line.split("\t")
            if len(cols) != 9:
                fout.write(raw_line)
                continue

            seqid, source, feature_type, start, end, score, strand, phase, attr_str = cols
            attr_pairs, attr_dict = parse_attrs(attr_str)

            if feature_type == "gene":
                gene_id = attr_dict.get("ID")
                if not gene_id:
                    fout.write(raw_line)
                    continue

                # write original gene row first
                fout.write(raw_line)

                # create synthetic mRNA row
                tx_id = f"transcript_{gene_id}"
                gene_to_tx[gene_id] = tx_id

                tx_attr_pairs = [
                    ("ID", tx_id),
                    ("Parent", gene_id),
                ]

                # carry over common descriptive fields if present
                for extra_key in ("Name", "gene", "gene_biotype", "locus_tag", "gbkey"):
                    if extra_key in attr_dict:
                        tx_attr_pairs.append((extra_key, attr_dict[extra_key]))

                tx_cols = [
                    seqid,
                    source,
                    "mRNA",
                    start,
                    end,
                    score,
                    strand,
                    ".",
                    build_attr_str(tx_attr_pairs),
                ]
                fout.write("\t".join(tx_cols) + "\n")

            elif feature_type == "CDS":
                parent = attr_dict.get("Parent")
                if parent in gene_to_tx:
                    new_parent = gene_to_tx[parent]
                    attr_pairs = replace_or_add_attr(attr_pairs, "Parent", new_parent)
                    cols[8] = build_attr_str(attr_pairs)
                    fout.write("\t".join(cols) + "\n")
                else:
                    fout.write(raw_line)

            else:
                fout.write(raw_line)


if __name__ == "__main__":
    main()