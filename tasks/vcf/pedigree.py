from tasks.vcf.experiment import Case


class Pedigree:
    """
    Represents a pedigree structure for a genetic case study, linking family roles
    (e.g., father, mother, progenies) to their corresponding experiments.

    Attributes:
       experiments (list[Case.Experiment]): A list of experiments corresponding to the VCF samples,
           ordered as they appear in the VCF file.
        father_experiment (Case.Experiment or None): The experiment associated with the father, if available.
        mother_experiment (Case.Experiment or None): The experiment associated with the mother, if available.
        is_father_affected (bool): Indicates if the father is affected by the condition.
        is_mother_affected (bool): Indicates if the mother is affected by the condition.
        father_seq_id (str or None): The sequence ID of the father, if available.
        mother_seq_id (str or None): The sequence ID of the mother, if available.
        progenies (list[Case.Experiment]): A list of experiments for progenies (e.g., proband, brother, sister).
        is_family (bool): Indicates if the pedigree represents a family (requires at least one parent and one progeny).

    Methods:
        __init__(case: Case, vcf_samples: list[str]):
            Initializes the Pedigree instance by mapping VCF samples to experiments
            and identifying family roles.
    """

    def __init__(self, case: Case, vcf_samples: list[str]):
        self.experiments = []
        # We save the experiments in the order of the samples in the VCF file
        for vcf_sample in vcf_samples:
            experiment = next(
                (exp for exp in case.experiments if exp.sample_id == vcf_sample), None
            )
            if experiment:
                self.experiments.append(experiment)

        self.father_experiment = next(
            (exp for exp in self.experiments if exp.family_role == "father"), None
        )
        self.mother_experiment = next(
            (exp for exp in self.experiments if exp.family_role == "mother"), None
        )
        self.is_father_affected = (
            self.father_experiment.is_affected if self.father_experiment else False
        )
        self.is_mother_affected = (
            self.mother_experiment.is_affected if self.mother_experiment else False
        )

        self.father_seq_id = (
            self.father_experiment.seq_id if self.father_experiment else None
        )
        self.mother_seq_id = (
            self.mother_experiment.seq_id if self.mother_experiment else None
        )

        self.progenies = [
            exp
            for exp in self.experiments
            if exp.family_role in ["proband", "brother", "sister"]
        ]

        self.is_family = (self.mother_seq_id or self.father_seq_id) and len(
            self.progenies
        ) > 0
